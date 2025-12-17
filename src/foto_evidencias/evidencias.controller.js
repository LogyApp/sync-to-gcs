// logyser-sync.js - VERSIÓN COMPLETA OPTIMIZADA
const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

// CONFIGURACIÓN
const CONFIG = {
    PROJECT_ID: process.env.GOOGLE_CLOUD_PROJECT || "eternal-brand-454501-i8",
    BUCKET_NAME: "logyser-cloud",
    MAX_PARALLEL_DOWNLOADS: 10,
    MAX_PARALLEL_UPLOADS: 15,
    PAGE_SIZE: 1000,
    FOLDERS: [
        { id: '1sWP0riOd96vSF9awPxF52Uu49QarWqAD', name: 'Foto_Consignaciones' },
        { id: '1CQlhcl4lMu3EU5GAYm_KIhV29gQb977H', name: 'Foto_Documentos' },
        { id: '1gjaeVCxkAsY4vxIU8UyU1dkuYn7R8UpL', name: 'Foto_Evidencias' },
        { id: '1iXypvkSj-om1Tx4q47qy75kMHlVUvpma', name: 'Foto_Transferencias' }
    ]
};

class LogySerSync {
    constructor() {
        this.storage = null;
        this.auth = null;
        this.token = null;
        this.tokenExpiry = null;
        this.LOG_PREFIX = '🟣 LOGYSER: ';
        this.totalStats = {
            success: 0,
            failed: 0,
            foldersProcessed: 0
        };
        this.IS_CLOUD_RUN = process.env.K_SERVICE !== undefined;
        this.IS_LOCAL = !this.IS_CLOUD_RUN;
    }

    // ============ MÉTODOS DE LOGGING ============
    log(message) {
        console.log(`${this.LOG_PREFIX}${message}`);
    }

    error(message) {
        console.error(`${this.LOG_PREFIX}❌ ${message}`);
    }

    success(message) {
        console.log(`${this.LOG_PREFIX}✅ ${message}`);
    }

    warn(message) {
        console.warn(`${this.LOG_PREFIX}⚠️  ${message}`);
    }

    info(message) {
        console.log(`${this.LOG_PREFIX}ℹ️  ${message}`);
    }

    // ============ INICIALIZACIÓN ============
    async initialize() {
        this.log('🚀 Inicializando LogySer Sync...');
        this.info(`🔍 Entorno: ${this.IS_CLOUD_RUN ? 'Cloud Run' : 'Local'}`);

        try {
            // 1. INICIALIZAR STORAGE Y AUTH SEGÚN ENTORNO
            if (this.IS_CLOUD_RUN) {
                await this.initializeForCloudRun();
            } else {
                await this.initializeForLocal();
            }

            // 2. VERIFICAR BUCKET
            await this.verifyAndCreateBucket();

            // 3. VERIFICAR AUTENTICACIÓN DRIVE
            await this.verifyDriveAuth();

            this.success('🎉 LogySer Sync inicializado correctamente');
            return true;

        } catch (error) {
            this.error(`❌ Error inicializando: ${error.message}`);
            throw error;
        }
    }

    async initializeForCloudRun() {
        this.info('☁️  Configurando para Cloud Run (ADC)');

        // Storage sin keyFilename en Cloud Run
        this.storage = new Storage({
            projectId: CONFIG.PROJECT_ID
        });

        // Auth sin keyFile en Cloud Run
        this.auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive.readonly'],
            projectId: CONFIG.PROJECT_ID
        });

        // Verificar ADC
        try {
            const credentials = await this.auth.getCredentials();
            this.success(`✅ ADC activo: ${credentials.client_email}`);
        } catch (adcError) {
            this.error(`❌ Error ADC: ${adcError.message}`);
            throw adcError;
        }
    }

    async initializeForLocal() {
        this.info('💻 Configurando para desarrollo local');
        const CREDENTIALS_PATH = './gcs-key.json';

        // Verificar archivo de credenciales
        if (!fs.existsSync(CREDENTIALS_PATH)) {
            throw new Error(`Archivo gcs-key.json no encontrado en: ${path.resolve(CREDENTIALS_PATH)}`);
        }

        // Storage con keyFilename en local
        this.storage = new Storage({
            projectId: CONFIG.PROJECT_ID,
            keyFilename: CREDENTIALS_PATH
        });

        // Auth con keyFile en local
        this.auth = new GoogleAuth({
            keyFile: CREDENTIALS_PATH,
            scopes: ['https://www.googleapis.com/auth/drive.readonly']
        });

        // Verificar credenciales locales
        try {
            const keyContent = JSON.parse(fs.readFileSync(CREDENTIALS_PATH, 'utf8'));
            this.success(`✅ Credenciales locales: ${keyContent.client_email}`);
        } catch (parseError) {
            throw new Error(`Error leyendo gcs-key.json: ${parseError.message}`);
        }
    }

    async verifyAndCreateBucket() {
        try {
            const [buckets] = await this.storage.getBuckets();
            const bucketExists = buckets.some(b => b.name === CONFIG.BUCKET_NAME);

            if (bucketExists) {
                this.success(`✅ Bucket encontrado: ${CONFIG.BUCKET_NAME}`);

                // Verificar acceso
                const [files] = await this.storage.bucket(CONFIG.BUCKET_NAME).getFiles({ maxResults: 1 });
                this.info(`📊 Archivos en bucket: ${files.length}`);
            } else {
                this.warn(`⚠️  Creando bucket: ${CONFIG.BUCKET_NAME}`);
                await this.storage.createBucket(CONFIG.BUCKET_NAME, {
                    location: 'us-central1',
                    storageClass: 'STANDARD'
                });
                this.success(`✅ Bucket creado: ${CONFIG.BUCKET_NAME}`);
            }
        } catch (error) {
            this.error(`⚠️  Error con bucket: ${error.message}`);
            // Continuar de todas formas
        }
    }

    async verifyDriveAuth() {
        try {
            if (!this.auth) {
                throw new Error('Auth no inicializado');
            }

            const client = await this.auth.getClient();
            const tokenResponse = await client.getAccessToken();

            if (tokenResponse?.token) {
                this.token = tokenResponse.token;
                this.tokenExpiry = Date.now() + (55 * 60 * 1000); // 55 minutos
                this.success('✅ Autenticación Drive verificada');
            } else {
                this.warn('⚠️  No se pudo obtener token inicial');
            }
        } catch (error) {
            this.error(`⚠️  Error verificando auth Drive: ${error.message}`);
            // No lanzar error, se manejará en getDriveToken()
        }
    }

    // ============ MANEJO DE TOKEN CON REINTENTOS ============
    async getDriveToken() {
        try {
            // Si el token es válido y no ha expirado, usarlo
            if (this.token && this.tokenExpiry && Date.now() < this.tokenExpiry) {
                return this.token;
            }

            this.log('🔄 Obteniendo token de Drive...');

            // Verificar que auth esté inicializado
            if (!this.auth) {
                await this.initialize();
            }

            const client = await this.auth.getClient();
            const tokenResponse = await client.getAccessToken();

            if (!tokenResponse?.token) {
                throw new Error('No se pudo obtener token de acceso');
            }

            // Guardar token con tiempo de expiración
            this.token = tokenResponse.token;
            this.tokenExpiry = Date.now() + (55 * 60 * 1000); // 55 minutos

            this.success('✅ Token Drive obtenido');
            return this.token;

        } catch (error) {
            this.error(`❌ Error obteniendo token: ${error.message}`);

            // Manejo específico del error 401
            if (error.message.includes('401') || error.message.includes('invalid_grant')) {

                if (this.IS_CLOUD_RUN) {
                    this.error('🔐 ERROR 401 EN CLOUD RUN:');
                    this.error('💡 Verificar:');
                    this.error('   1. Service Account tiene rol "roles/drive.reader"');
                    this.error('   2. Carpetas compartidas con la Service Account');
                    this.error('   3. Reiniciar servicio después de cambios');
                } else {
                    this.error('🔐 ERROR 401 EN LOCAL:');
                    this.error('💡 Verificar:');
                    this.error('   1. Archivo gcs-key.json válido');
                    this.error('   2. Credenciales no han expirado');
                    this.error('   3. Carpetas compartidas con la cuenta de servicio');
                }

                // Limpiar token inválido
                this.token = null;
                this.tokenExpiry = null;
            }

            throw error;
        }
    }

    // ============ UTILIDADES DRIVE ============
    async listAllItems(folderId, token, pageToken = null) {
        const allItems = [];

        try {
            const url = `https://www.googleapis.com/drive/v3/files?` +
                `q='${folderId}'+in+parents+and+trashed=false&` +
                `fields=nextPageToken,files(id,name,mimeType,size,modifiedTime,createdTime)&` +
                `pageSize=${CONFIG.PAGE_SIZE}` +
                (pageToken ? `&pageToken=${pageToken}` : '');

            const response = await fetch(url, {
                headers: { Authorization: `Bearer ${token}` }
            });

            if (!response.ok) {
                const errorText = await response.text();

                // Manejo específico del error 401
                if (response.status === 401) {
                    throw new Error(`Token expirado (401): ${errorText.substring(0, 200)}`);
                }

                throw new Error(`Drive API error ${response.status}: ${errorText.substring(0, 200)}`);
            }

            const data = await response.json();
            if (data.files && data.files.length > 0) {
                allItems.push(...data.files);
            }

            // Si hay más páginas, seguir obteniendo
            if (data.nextPageToken) {
                const moreFiles = await this.listAllItems(folderId, token, data.nextPageToken);
                allItems.push(...moreFiles);
            }

            return allItems;

        } catch (error) {
            this.error(`Error listando items en ${folderId}: ${error.message}`);
            throw error;
        }
    }

    async downloadFile(fileId, mimeType, token) {
        let url;

        if (mimeType && mimeType.includes('application/vnd.google-apps')) {
            url = `https://www.googleapis.com/drive/v3/files/${fileId}/export?mimeType=application/pdf`;
        } else {
            url = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`;
        }

        try {
            const response = await fetch(url, {
                headers: { Authorization: `Bearer ${token}` }
            });

            if (!response.ok) {
                throw new Error(`Error ${response.status}: ${response.statusText}`);
            }

            const buffer = await response.buffer();
            return buffer;

        } catch (error) {
            throw new Error(`Error descargando archivo: ${error.message}`);
        }
    }

    // ============ PROCESAMIENTO MASIVO PARALELO ============
    async processFilesInParallel(files, token, basePath) {
        const results = { success: 0, failed: 0 };
        const semaphore = { count: 0 };

        this.info(`📥 Procesando ${files.length} archivos en paralelo (${CONFIG.MAX_PARALLEL_DOWNLOADS} concurrentes)`);

        const processSingleFile = async (file) => {
            // Control de concurrencia
            while (semaphore.count >= CONFIG.MAX_PARALLEL_DOWNLOADS) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }

            semaphore.count++;

            try {
                // 1. Descargar archivo
                const content = await this.downloadFile(file.id, file.mimeType, token);

                // 2. Subir a GCS
                const destinationPath = `${basePath}${file.name}`;
                await this.uploadToGCS(destinationPath, content, file.mimeType);

                results.success++;
                this.log(`   ✅ ${file.name} (${(content.length / 1024 / 1024).toFixed(2)} MB)`);

            } catch (error) {
                results.failed++;
                this.error(`   ❌ ${file.name}: ${error.message}`);

            } finally {
                semaphore.count--;
            }
        };

        // Ejecutar todos los procesos en paralelo
        const promises = files.map(file => processSingleFile(file));
        await Promise.all(promises);

        return results;
    }

    async uploadToGCS(destinationPath, content, contentType) {
        try {
            const file = this.storage.bucket(CONFIG.BUCKET_NAME).file(destinationPath);

            // Verificar si ya existe
            const [exists] = await file.exists();
            if (exists) {
                return true; // Ya existe, no subir de nuevo
            }

            await file.save(content, {
                metadata: {
                    contentType: contentType || 'application/octet-stream'
                }
            });

            return true;

        } catch (error) {
            throw new Error(`Error subiendo ${destinationPath}: ${error.message}`);
        }
    }

    // ============ PROCESAMIENTO RECURSIVO OPTIMIZADO ============
    async processFolderRecursively(folderId, currentPath, token, maxDepth = 10, currentDepth = 0) {
        if (currentDepth >= maxDepth) {
            this.warn(`⏹️  Profundidad máxima alcanzada: ${currentPath}`);
            return { success: 0, failed: 0, folders: 0, files: 0 };
        }

        const stats = { success: 0, failed: 0, folders: 0, files: 0 };

        try {
            this.log(`📂 Explorando: ${currentPath || 'raíz'}`);

            // Listar todos los items
            const allItems = await this.listAllItems(folderId, token);

            if (allItems.length === 0) {
                this.log('📭 Carpeta vacía');
                return stats;
            }

            // Separar carpetas y archivos
            const folders = allItems.filter(item => item.mimeType === 'application/vnd.google-apps.folder');
            const files = allItems.filter(item => item.mimeType !== 'application/vnd.google-apps.folder');

            this.info(`📊 Encontrados: ${files.length} archivos, ${folders.length} carpetas`);

            // 🔥 PROCESAR ARCHIVOS EN PARALELO (MASIVO)
            if (files.length > 0) {
                const fileResults = await this.processFilesInParallel(files, token, currentPath);
                stats.success += fileResults.success;
                stats.failed += fileResults.failed;
                stats.files += files.length;
            }

            // 🔥 PROCESAR SUBCARPETAS EN PARALELO (limitado)
            if (folders.length > 0) {
                const MAX_PARALLEL_FOLDERS = 5;

                for (let i = 0; i < folders.length; i += MAX_PARALLEL_FOLDERS) {
                    const batch = folders.slice(i, i + MAX_PARALLEL_FOLDERS);

                    const batchPromises = batch.map(async (folder) => {
                        const subStats = await this.processFolderRecursively(
                            folder.id,
                            `${currentPath}${folder.name}/`,
                            token,
                            maxDepth,
                            currentDepth + 1
                        );
                        return subStats;
                    });

                    const batchResults = await Promise.all(batchPromises);

                    batchResults.forEach(subStats => {
                        stats.success += subStats.success;
                        stats.failed += subStats.failed;
                        stats.folders += subStats.folders + 1; // +1 por la carpeta actual
                        stats.files += subStats.files;
                    });

                    this.info(`   📈 Carpetas procesadas: ${Math.min(i + MAX_PARALLEL_FOLDERS, folders.length)}/${folders.length}`);
                }
            }

        } catch (error) {
            this.error(`❌ Error en ${currentPath}: ${error.message}`);
            stats.failed += files.length; // Contar todos los archivos como fallidos
        }

        return stats;
    }

    // ============ SINCRONIZACIÓN COMPLETA ============
    async syncAll(forceFullSync = false) {
        this.log('\n🚀 ========================================');
        this.log('🚀 INICIANDO SINCRONIZACIÓN MASIVA LOGYSER');
        this.log(`🚀 Entorno: ${this.IS_CLOUD_RUN ? 'Cloud Run' : 'Local'}`);
        this.log(`🚀 Paralelismo: ${CONFIG.MAX_PARALLEL_DOWNLOADS} descargas concurrentes`);
        this.log('🚀 ========================================');

        // Reiniciar estadísticas
        this.totalStats = { success: 0, failed: 0, foldersProcessed: 0 };
        const startTime = Date.now();

        try {
            // Inicializar si es necesario
            if (!this.storage || !this.auth) {
                await this.initialize();
            }

            // Obtener token con manejo de error 401
            let token;
            try {
                token = await this.getDriveToken();
            } catch (tokenError) {
                this.error(`❌ No se pudo obtener token: ${tokenError.message}`);

                // Si es error 401, intentar un último reintento
                if (tokenError.message.includes('401')) {
                    this.log('🔄 Intentando último reintento en 15 segundos...');
                    await new Promise(resolve => setTimeout(resolve, 15000));
                    token = await this.getDriveToken();
                } else {
                    throw tokenError;
                }
            }

            // Sincronizar cada carpeta
            for (const folder of CONFIG.FOLDERS) {
                this.log(`\n📁 ${'='.repeat(50)}`);
                this.log(`📁 CARPETA: ${folder.name}`);
                this.log(`📁 ID: ${folder.id}`);
                this.log(`📁 ${'='.repeat(50)}`);

                try {
                    const stats = await this.processFolderRecursively(
                        folder.id,
                        `${folder.name}/`,
                        token
                    );

                    this.totalStats.success += stats.success;
                    this.totalStats.failed += stats.failed;
                    this.totalStats.foldersProcessed += stats.folders + 1;

                    this.info(`📊 Resultado ${folder.name}:`);
                    this.info(`   ✅ Archivos exitosos: ${stats.success}`);
                    this.info(`   ❌ Archivos fallidos: ${stats.failed}`);
                    this.info(`   📁 Subcarpetas: ${stats.folders}`);
                    this.info(`   📄 Total procesado: ${stats.files}`);

                } catch (folderError) {
                    this.error(`❌ Error crítico en ${folder.name}: ${folderError.message}`);
                    // Continuar con la siguiente carpeta
                }

                // Pequeña pausa entre carpetas
                if (folder !== CONFIG.FOLDERS[CONFIG.FOLDERS.length - 1]) {
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            }

            const elapsedTime = Date.now() - startTime;

            // RESUMEN FINAL
            this.log('\n🎉 ========================================');
            this.log('🎉 RESUMEN FINAL - PROCESAMIENTO MASIVO');
            this.log('🎉 ========================================');

            if (this.totalStats.success > 0) {
                this.success(`✅ Archivos exitosos: ${this.totalStats.success}`);
            }

            if (this.totalStats.failed > 0) {
                this.error(`❌ Archivos fallidos: ${this.totalStats.failed}`);
            }

            this.info(`📁 Carpetas procesadas: ${this.totalStats.foldersProcessed}`);
            this.info(`📦 Bucket destino: ${CONFIG.BUCKET_NAME}`);
            this.info(`⚡ Paralelismo: ${CONFIG.MAX_PARALLEL_DOWNLOADS} descargas concurrentes`);
            this.info(`🕒 Tiempo total: ${(elapsedTime / 1000).toFixed(2)} segundos`);
            this.info(`📅 Finalizado: ${new Date().toLocaleString()}`);

            return {
                success: true,
                totalSuccess: this.totalStats.success,
                totalFailed: this.totalStats.failed,
                totalFolders: this.totalStats.foldersProcessed,
                elapsedTime: elapsedTime,
                environment: this.IS_CLOUD_RUN ? 'cloud-run' : 'local'
            };

        } catch (error) {
            this.error(`❌ ERROR CRÍTICO en sincronización: ${error.message}`);

            return {
                success: false,
                error: error.message,
                totalSuccess: this.totalStats.success,
                totalFailed: this.totalStats.failed,
                environment: this.IS_CLOUD_RUN ? 'cloud-run' : 'local'
            };
        }
    }

    // ============ FUNCIÓN DE DIAGNÓSTICO ============
    async diagnose() {
        this.log('\n🔍 ========================================');
        this.log('🔍 DIAGNÓSTICO LOGYSER SYNC');
        this.log('🔍 ========================================');

        const results = {
            environment: this.IS_CLOUD_RUN ? 'Cloud Run' : 'Local',
            timestamp: new Date().toISOString(),
            checks: {}
        };

        try {
            // 1. Verificar entorno
            results.checks.environment = {
                K_SERVICE: process.env.K_SERVICE || 'No definido',
                GOOGLE_CLOUD_PROJECT: process.env.GOOGLE_CLOUD_PROJECT || 'No definido',
                NODE_ENV: process.env.NODE_ENV || 'No definido'
            };

            // 2. Inicializar
            await this.initialize();
            results.checks.initialization = '✅ OK';

            // 3. Verificar token
            const token = await this.getDriveToken();
            results.checks.token = token ? `✅ Obtenido (${token.length} chars)` : '❌ Falló';

            // 4. Verificar bucket
            const [bucketExists] = await this.storage.bucket(CONFIG.BUCKET_NAME).exists();
            results.checks.bucket = bucketExists ? '✅ Existe' : '❌ No existe';

            // 5. Verificar acceso a Drive
            try {
                const testUrl = 'https://www.googleapis.com/drive/v3/about?fields=user';
                const response = await fetch(testUrl, {
                    headers: { Authorization: `Bearer ${token}` }
                });
                results.checks.driveAccess = response.ok ? '✅ Acceso concedido' : `❌ Error ${response.status}`;
            } catch (driveError) {
                results.checks.driveAccess = `❌ ${driveError.message}`;
            }

            this.success('✅ Diagnóstico completado');
            return results;

        } catch (error) {
            results.error = error.message;
            this.error(`❌ Error en diagnóstico: ${error.message}`);
            return results;
        }
    }
}

// Exportar instancia singleton
const logyserSync = new LogySerSync();
module.exports = logyserSync;
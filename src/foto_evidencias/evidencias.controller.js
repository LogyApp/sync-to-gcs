// logyser-sync.js - VERSIÓN COMPATIBLE CON CLOUD RUN
const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

// CONFIGURACIÓN ADAPTATIVA
const IS_CLOUD_RUN = process.env.K_SERVICE !== undefined;
const LOCAL_CREDENTIALS_PATH = './gcs-key.json';

const CONFIG = {
    PROJECT_ID: process.env.GOOGLE_CLOUD_PROJECT || "eternal-brand-454501-i8",
    BUCKET_NAME: "logyser-cloud",

    // Carpetas específicas de LogySer
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
        this.LOG_PREFIX = '🟣 LOGYSER: ';
        this.totalStats = {
            success: 0,
            failed: 0,
            foldersProcessed: 0
        };
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

        // Detectar entorno
        const IS_CLOUD_RUN = process.env.K_SERVICE !== undefined;
        const IS_LOCAL = !IS_CLOUD_RUN;
        const CREDENTIALS_PATH = process.env.GCS_KEY_PATH || './gcs-key.json';

        this.info(`📁 Entorno: ${IS_CLOUD_RUN ? 'Cloud Run' : 'Local'}`);
        this.info(`📁 Proyecto: ${CONFIG.PROJECT_ID}`);
        this.info(`📁 Ruta credenciales: ${path.resolve(CREDENTIALS_PATH)}`);

        if (IS_LOCAL) {
            this.info(`📊 Existe archivo: ${fs.existsSync(CREDENTIALS_PATH)}`);
        }

        try {
            // ============ CONFIGURACIÓN ADAPTATIVA POR ENTORNO ============
            let storageConfig = { projectId: CONFIG.PROJECT_ID };
            let authConfig = {
                scopes: [
                    'https://www.googleapis.com/auth/drive',
                    'https://www.googleapis.com/auth/drive.readonly',
                    'https://www.googleapis.com/auth/drive.file'
                ]
            };

            if (IS_CLOUD_RUN) {
                // ============ EN CLOUD RUN ============
                this.info('🔑 Usando Application Default Credentials (Cloud Run)');

                // Para Cloud Run, ADC se configura automáticamente
                // Solo necesitamos el projectId
                storageConfig = { projectId: CONFIG.PROJECT_ID };
                authConfig = {
                    scopes: [
                        'https://www.googleapis.com/auth/drive',
                        'https://www.googleapis.com/auth/drive.readonly',
                        'https://www.googleapis.com/auth/drive.file'
                    ]
                };

            } else {
                // ============ EN DESARROLLO LOCAL ============
                this.info('🔑 Usando credenciales locales');

                // Verificar que exista el archivo de credenciales
                if (!fs.existsSync(CREDENTIALS_PATH)) {
                    this.error(`❌ Archivo de credenciales no encontrado: ${CREDENTIALS_PATH}`);
                    this.error('💡 Soluciones posibles:');
                    this.error('   1. Crea un archivo gcs-key.json en la raíz del proyecto');
                    this.error('   2. Establece la variable de entorno GCS_KEY_PATH con la ruta correcta');
                    this.error('   3. Exporta GOOGLE_APPLICATION_CREDENTIALS con la ruta del archivo');
                    throw new Error(`Archivo de credenciales no encontrado: ${CREDENTIALS_PATH}`);
                }

                // Configurar con archivo de credenciales
                storageConfig.keyFilename = CREDENTIALS_PATH;
                authConfig.keyFile = CREDENTIALS_PATH;
            }

            // ============ INICIALIZAR GOOGLE CLOUD STORAGE ============
            this.log('📦 Inicializando Google Cloud Storage...');
            this.storage = new Storage(storageConfig);

            // Verificar conexión listando buckets
            const [buckets] = await this.storage.getBuckets();
            this.success(`✅ Storage inicializado. Buckets disponibles: ${buckets.length}`);

            if (buckets.length > 0) {
                this.info(`📋 Buckets: ${buckets.slice(0, 3).map(b => b.name).join(', ')}${buckets.length > 3 ? '...' : ''}`);
            }

            // ============ VERIFICAR/CREAR BUCKET LOGYSER ============
            this.log(`🔍 Verificando bucket: ${CONFIG.BUCKET_NAME}`);
            const bucketExists = buckets.some(b => b.name === CONFIG.BUCKET_NAME);

            if (!bucketExists) {
                this.warn(`🆕 Bucket no encontrado, creando: ${CONFIG.BUCKET_NAME}`);

                try {
                    await this.storage.createBucket(CONFIG.BUCKET_NAME, {
                        location: 'us-central1',
                        storageClass: 'STANDARD',
                        versioning: {
                            enabled: false
                        }
                    });
                    this.success(`✅ Bucket creado exitosamente: ${CONFIG.BUCKET_NAME}`);
                } catch (createError) {
                    this.error(`❌ Error creando bucket: ${createError.message}`);

                    // Si no tiene permisos para crear, verificar si puede usar otro bucket
                    if (createError.code === 403) {
                        this.error('🚫 Permisos insuficientes para crear bucket');
                        this.error('💡 Verifica que la cuenta de servicio tenga el rol: roles/storage.admin');
                        throw createError;
                    }
                    throw createError;
                }
            } else {
                this.success(`✅ Bucket encontrado: ${CONFIG.BUCKET_NAME}`);

                // Verificar permisos de escritura
                try {
                    const bucket = this.storage.bucket(CONFIG.BUCKET_NAME);
                    const [files] = await bucket.getFiles({ maxResults: 1 });
                    this.info(`📊 Archivos en bucket: ${files.length} (acceso verificado)`);
                } catch (accessError) {
                    this.error(`❌ Error accediendo al bucket: ${accessError.message}`);
                    this.error('💡 Verifica permisos de lectura/escritura en el bucket');
                    throw accessError;
                }
            }

            // ============ INICIALIZAR AUTENTICACIÓN PARA GOOGLE DRIVE ============
            this.log('🔑 Inicializando autenticación para Google Drive...');

            try {
                this.auth = new GoogleAuth(authConfig);

                // Probar autenticación obteniendo un token
                const client = await this.auth.getClient();
                const token = await client.getAccessToken();

                if (token && token.token) {
                    this.success(`✅ Autenticación Drive exitosa (token obtenido)`);
                    if (IS_LOCAL) {
                        this.info(`   Token: ${token.token.substring(0, 30)}...`);
                    }
                } else {
                    this.error('❌ No se pudo obtener token de Drive');
                    throw new Error('Fallo en autenticación con Google Drive');
                }

            } catch (authError) {
                this.error(`❌ Error en autenticación Drive: ${authError.message}`);

                // Mensajes de ayuda específicos
                if (authError.message.includes('invalid_grant')) {
                    this.error('💡 Posibles soluciones:');
                    this.error('   1. Verifica que las credenciales no hayan expirado');
                    this.error('   2. Asegúrate de que la cuenta de servicio tenga acceso a Drive');
                    this.error('   3. Comparte las carpetas de LogySer con la cuenta de servicio');
                } else if (authError.message.includes('ENOENT')) {
                    this.error('💡 Archivo de credenciales no encontrado');
                    this.error(`   Ruta buscada: ${CREDENTIALS_PATH}`);
                }

                throw authError;
            }

            // ============ INICIALIZACIÓN COMPLETADA ============
            this.success('🎉 LogySer Sync inicializado correctamente');
            this.info(`📁 Carpetas configuradas: ${CONFIG.FOLDERS.length}`);
            this.info(`📦 Bucket destino: ${CONFIG.BUCKET_NAME}`);
            this.info(`🏢 Proyecto GCP: ${CONFIG.PROJECT_ID}`);

            if (IS_CLOUD_RUN) {
                this.info(`☁️  Entorno: Cloud Run (${process.env.K_SERVICE})`);
            } else {
                this.info(`💻 Entorno: Desarrollo Local`);
            }

            return true;

        } catch (error) {
            this.error(`❌ Error inicializando LogySer Sync: ${error.message}`);

            // Información adicional para debugging
            this.error(`🔧 Debug info:`);
            this.error(`   - Entorno: ${IS_CLOUD_RUN ? 'Cloud Run' : 'Local'}`);
            this.error(`   - Project ID: ${CONFIG.PROJECT_ID}`);
            this.error(`   - Credentials path: ${CREDENTIALS_PATH}`);
            this.error(`   - File exists: ${fs.existsSync(CREDENTIALS_PATH)}`);
            this.error(`   - K_SERVICE env: ${process.env.K_SERVICE || 'No definido'}`);

            if (error.stack) {
                this.error(`   - Stack: ${error.stack.split('\n')[1]?.trim() || 'N/A'}`);
            }

            throw error;
        }
    }

    // ============ AUTENTICACIÓN DRIVE ============
    async getDriveToken() {
        try {
            if (this.token) {
                return this.token;
            }
            const client = await this.auth.getClient();
            const token = await client.getAccessToken();
            this.token = token.token;
            this.success(`Token Drive obtenido (${this.token.substring(0, 20)}...)`);
            return this.token;
        } catch (error) {
            this.error(`Error obteniendo token Drive: ${error.message}`);
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
                `pageSize=1000` +
                (pageToken ? `&pageToken=${pageToken}` : '');

            const response = await fetch(url, {
                headers: { Authorization: `Bearer ${token}` }
            });

            if (!response.ok) {
                const errorText = await response.text();
                this.error(`Error Drive API: ${response.status} - ${errorText.substring(0, 100)}`);
                throw new Error(`Drive API error ${response.status}`);
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
            this.log(`      📥 Descargado: ${buffer.length} bytes`);
            return buffer;

        } catch (error) {
            throw new Error(`Error descargando archivo: ${error.message}`);
        }
    }

    // ============ UTILIDADES GCS ============
    async uploadToGCS(destinationPath, content, contentType) {
        try {
            const file = this.storage.bucket(CONFIG.BUCKET_NAME).file(destinationPath);

            // Verificar si ya existe
            const [exists] = await file.exists();
            if (exists) {
                this.log(`      ⏭️  Ya existe: ${destinationPath}`);
                return true;
            }

            this.log(`      ⬆️  Subiendo: ${destinationPath} (${content.length} bytes)`);

            await file.save(content, {
                metadata: {
                    contentType: contentType || 'application/octet-stream'
                }
            });

            this.success(`Subido: ${destinationPath}`);
            return true;

        } catch (error) {
            throw new Error(`Error subiendo ${destinationPath}: ${error.message}`);
        }
    }

    async fileExistsInGCS(filePath) {
        try {
            const file = this.storage.bucket(CONFIG.BUCKET_NAME).file(filePath);
            const [exists] = await file.exists();
            return exists;
        } catch (error) {
            this.warn(`Error verificando existencia de ${filePath}: ${error.message}`);
            return false;
        }
    }

    // ============ PROCESAMIENTO RECURSIVO ============
    async processFolderRecursively(folderId, currentPath, token, maxDepth = 10, currentDepth = 0, modifiedSince = null) {
        if (currentDepth >= maxDepth) {
            this.warn(`      ⏹️  Profundidad máxima (${maxDepth}) alcanzada en: ${currentPath}`);
            return { success: 0, failed: 0, folders: 0, files: 0 };
        }

        const stats = {
            success: 0,
            failed: 0,
            folders: 0,
            files: 0
        };
        const indent = '  '.repeat(currentDepth);

        try {
            this.log(`${indent}🔍 Nivel ${currentDepth}: Explorando ${currentPath || 'raíz'}`);

            // Obtener TODOS los archivos (sin filtro)
            const allItems = await this.listAllItems(folderId, token);

            if (allItems.length === 0) {
                this.log(`${indent}📭 Carpeta vacía`);
                return stats;
            }

            this.info(`${indent}📊 Encontrados ${allItems.length} items totales`);

            // Separar carpetas y archivos
            const folders = allItems.filter(item => item.mimeType === 'application/vnd.google-apps.folder');
            const files = allItems.filter(item => item.mimeType !== 'application/vnd.google-apps.folder');

            this.info(`${indent}📁 Carpetas: ${folders.length}, 📄 Archivos: ${files.length}`);

            // 🔥 PROCESAR TODOS LOS ARCHIVOS (no filtrar por tiempo)
            for (const file of files) {
                try {
                    this.log(`${indent}  📄 Procesando archivo: ${file.name} (${file.mimeType})`);
                    stats.files++;

                    const content = await this.downloadFile(file.id, file.mimeType, token);
                    const destinationPath = `${currentPath}${file.name}`;

                    const uploaded = await this.uploadToGCS(destinationPath, content, file.mimeType);
                    if (uploaded) {
                        stats.success++;
                    }

                } catch (error) {
                    this.error(`${indent}  ❌ Error con ${file.name}: ${error.message}`);
                    stats.failed++;
                }
            }

            // Procesar TODAS las subcarpetas
            for (const folder of folders) {
                this.log(`${indent}  📁 Procesando subcarpeta: ${folder.name}`);
                stats.folders++;

                const subStats = await this.processFolderRecursively(
                    folder.id,
                    `${currentPath}${folder.name}/`,
                    token,
                    maxDepth,
                    currentDepth + 1,
                    modifiedSince
                );

                stats.success += subStats.success;
                stats.failed += subStats.failed;
                stats.folders += subStats.folders;
                stats.files += subStats.files;
            }

            this.info(`${indent}📈 Resumen nivel ${currentDepth}:`);
            this.info(`${indent}  ✅ Archivos exitosos: ${stats.success}`);
            this.info(`${indent}  ❌ Archivos fallidos: ${stats.failed}`);
            this.info(`${indent}  📁 Subcarpetas: ${stats.folders}`);
            this.info(`${indent}  📄 Total items procesados: ${stats.files}`);

        } catch (error) {
            this.error(`${indent}🚨 Error explorando ${currentPath}: ${error.message}`);
        }

        return stats;
    }

    // ============ SINCRONIZACIÓN DE CARPETA ============
    async syncFolder(folder, token, forceFullSync = false) {
        const { id, name } = folder;

        this.log(`\n🎯 ========================================`);
        this.log(`🎯 PROCESANDO: ${name}`);
        this.log(`🎯 ID: ${id}`);
        this.log(`🎯 ========================================`);

        try {
            // TEMPORAL: Para probar, usar siempre sincronización completa
            // TODO: Implementar lógica de estado después
            const modifiedSince = forceFullSync ? '2000-01-01T00:00:00.000Z' : '2000-01-01T00:00:00.000Z';

            if (forceFullSync) {
                this.log(`🔧 MODO FORZADO: Sincronizando TODO`);
            } else {
                this.log(`🔧 Modo normal (sin estado aún)`);
            }

            // Procesar recursivamente
            this.log(`\n🔄 INICIANDO PROCESAMIENTO RECURSIVO...`);
            const stats = await this.processFolderRecursively(id, `${name}/`, token, 10, 0, modifiedSince);

            // Guardar estado (por ahora siempre fecha actual)
            const newSyncTime = new Date().toISOString();
            await this.saveFolderSyncState(id, newSyncTime);

            this.log(`\n📊 RESUMEN ${name}:`);
            this.success(`Archivos subidos: ${stats.success}`);
            if (stats.failed > 0) {
                this.error(`Archivos fallidos: ${stats.failed}`);
            }
            this.info(`Subcarpetas exploradas: ${stats.folders}`);
            this.info(`Archivos encontrados: ${stats.files}`);
            this.info(`Última sincronización: ${newSyncTime}`);
            this.info(`Ruta en GCS: ${CONFIG.BUCKET_NAME}/${name}/`);

            // Actualizar estadísticas globales
            this.totalStats.success += stats.success;
            this.totalStats.failed += stats.failed;
            this.totalStats.foldersProcessed += stats.folders;

            return stats;

        } catch (error) {
            this.error(`Error procesando ${name}: ${error.message}`);
            return { success: 0, failed: 0, folders: 0, files: 0 };
        }
    }

    // ============ SINCRONIZACIÓN COMPLETA ============
    async syncAll(forceFullSync = false) {
        this.log('\n🚀 ========================================');
        this.log(`🚀 INICIANDO SINCRONIZACIÓN LOGYSER ${forceFullSync ? 'COMPLETA' : 'INCREMENTAL'}`);
        this.log('🚀 ========================================');
        this.info(`📦 Bucket destino: ${CONFIG.BUCKET_NAME}`);

        if (forceFullSync) {
            this.warn('🔧 MODO FORZADO: Sincronizando TODOS los archivos (no solo nuevos)');
        }

        // Reiniciar estadísticas
        this.totalStats = {
            success: 0,
            failed: 0,
            foldersProcessed: 0
        };

        const startTime = Date.now();

        try {
            // Inicializar si no está hecho
            if (!this.storage || !this.auth) {
                this.warn('🔧 Servicios no inicializados, inicializando...');
                await this.initialize();
            }

            // Obtener token
            const token = await this.getDriveToken();
            const results = {
                timestamp: new Date().toISOString(),
                mode: forceFullSync ? 'full' : 'incremental',
                total: { success: 0, failed: 0, folders: 0 },
                folders: {}
            };

            // Sincronizar cada carpeta
            for (const folder of CONFIG.FOLDERS) {
                this.log(`\n🌟 ${'='.repeat(50)}`);
                this.log(`🌟 CARPETA: ${folder.name}`);
                this.log(`🌟 ID: ${folder.id}`);
                this.log(`🌟 ${'='.repeat(50)}`);

                const folderStats = await this.syncFolder(folder, token, forceFullSync);

                results.folders[folder.name] = folderStats;
                results.total.success += folderStats.success;
                results.total.failed += folderStats.failed;
                results.total.folders += folderStats.folders;

                // Pequeña pausa entre carpetas
                if (folder !== CONFIG.FOLDERS[CONFIG.FOLDERS.length - 1]) {
                    await new Promise(resolve => setTimeout(resolve, 1000));
                }
            }

            const elapsedTime = Date.now() - startTime;

            // Resumen final
            this.log('\n🎉 ========================================');
            this.log('🎉 RESUMEN FINAL LOGYSER');
            this.log('🎉 ========================================');
            this.success(`✅ Total exitosos: ${results.total.success}`);
            if (results.total.failed > 0) {
                this.error(`❌ Total fallidos: ${results.total.failed}`);
            }
            this.info(`📁 Total carpetas procesadas: ${results.total.folders}`);
            this.info(`📦 Bucket: ${CONFIG.BUCKET_NAME}`);
            this.info(`🕒 Tiempo total: ${(elapsedTime / 1000).toFixed(2)} segundos`);
            this.info(`📅 Fecha: ${new Date().toLocaleString()}`);
            this.info(`🔧 Modo: ${results.mode}`);

            this.log(`\n📁 DETALLE POR CARPETA:`);
            for (const [name, stats] of Object.entries(results.folders)) {
                const status = stats.failed > 0 ? '⚠️' : '✅';
                this.log(`   ${status} ${name}: ${stats.success} archivos, ${stats.folders} subcarpetas`);
            }

            return {
                success: true,
                timestamp: results.timestamp,
                total: results.total,
                folders: results.folders,
                elapsedTime: elapsedTime,
                mode: results.mode
            };

        } catch (error) {
            this.error(`ERROR CRÍTICO en sincronización: ${error.message}`);
            this.error(`Stack: ${error.stack}`);

            return {
                success: false,
                error: error.message,
                timestamp: new Date().toISOString(),
                total: this.totalStats
            };
        }
    }

    // ============ FUNCIÓN DE TEST ============
    async testAccess() {
        this.log('\n🔍 TESTEANDO ACCESO A CARPETAS LOGYSER');

        try {
            if (!this.auth) {
                await this.initialize();
            }

            const token = await this.getDriveToken();

            for (const folder of CONFIG.FOLDERS) {
                this.log(`\n📂 Probando: ${folder.name}`);
                this.log(`   ID: ${folder.id}`);

                try {
                    const url = `https://www.googleapis.com/drive/v3/files/${folder.id}?fields=id,name,mimeType`;
                    const response = await fetch(url, {
                        headers: { Authorization: `Bearer ${token}` }
                    });

                    if (response.ok) {
                        const data = await response.json();
                        this.success(`   ✅ ACCESO CONCEDIDO: ${data.name}`);

                        // Listar algunos items
                        const listUrl = `https://www.googleapis.com/drive/v3/files?q='${folder.id}'+in+parents&fields=files(id,name,mimeType)&pageSize=3`;
                        const listResponse = await fetch(listUrl, {
                            headers: { Authorization: `Bearer ${token}` }
                        });

                        if (listResponse.ok) {
                            const listData = await listResponse.json();
                            this.info(`   📊 Items: ${listData.files?.length || 0}`);
                        }
                    } else {
                        this.error(`   ❌ ERROR ${response.status}: Sin acceso`);
                    }
                } catch (err) {
                    this.error(`   🚨 EXCEPCIÓN: ${err.message}`);
                }
            }
        } catch (error) {
            this.error(`Error en test: ${error.message}`);
        }
    }
}

// Exportar una instancia singleton
const logyserSync = new LogySerSync();
module.exports = logyserSync;
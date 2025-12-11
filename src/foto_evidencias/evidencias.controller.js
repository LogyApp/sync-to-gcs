// logyser-sync.js - VERSIÓN CON DEPURACIÓN PROFUNDA
const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

// CONFIGURACIÓN ESPECÍFICA
const CONFIG = {
    PROJECT_ID: "eternal-brand-454501-i8",
    BUCKET_NAME: "logyser-cloud",
    CREDENTIALS_PATH: './gcs-key.json',

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
    }

    async initialize() {
        console.log('🚀 Inicializando LogySer Sync...');

        try {
            // Inicializar Storage
            this.storage = new Storage({
                projectId: CONFIG.PROJECT_ID,
                keyFilename: CONFIG.CREDENTIALS_PATH
            });

            // Verificar bucket
            const [buckets] = await this.storage.getBuckets();
            const bucketExists = buckets.some(b => b.name === CONFIG.BUCKET_NAME);

            if (!bucketExists) {
                console.log(`🆕 Creando bucket: ${CONFIG.BUCKET_NAME}`);
                await this.storage.createBucket(CONFIG.BUCKET_NAME);
                console.log(`✅ Bucket creado: ${CONFIG.BUCKET_NAME}`);
            } else {
                console.log(`✅ Bucket encontrado: ${CONFIG.BUCKET_NAME}`);
            }

            // Inicializar Auth para Drive
            this.auth = new GoogleAuth({
                keyFile: CONFIG.CREDENTIALS_PATH,
                scopes: ['https://www.googleapis.com/auth/drive']
            });

            console.log(`✅ LogySer Sync inicializado correctamente`);

        } catch (error) {
            console.error('❌ Error inicializando LogySer Sync:', error.message);
            throw error;
        }
    }

    async getDriveToken() {
        try {
            if (this.token) {
                return this.token;
            }
            const client = await this.auth.getClient();
            const token = await client.getAccessToken();
            this.token = token.token;
            console.log(`🔑 Token Drive obtenido`);
            return this.token;
        } catch (error) {
            console.error('❌ Error obteniendo token Drive:', error.message);
            throw error;
        }
    }

    async testFolderAccess(folderId, folderName, token) {
        console.log(`\n🔍 TESTEANDO ACCESO A: ${folderName}`);
        console.log(`   ID: ${folderId}`);

        try {
            // Test 1: Verificar si la carpeta existe
            const infoUrl = `https://www.googleapis.com/drive/v3/files/${folderId}?fields=id,name,mimeType,createdTime,modifiedTime`;
            const infoResponse = await fetch(infoUrl, {
                headers: { Authorization: `Bearer ${token}` }
            });

            if (!infoResponse.ok) {
                console.log(`   ❌ ERROR ${infoResponse.status}: No se puede acceder a la carpeta`);
                console.log(`   Detalles: ${await infoResponse.text()}`);
                return false;
            }

            const folderInfo = await infoResponse.json();
            console.log(`   ✅ CARPETA ENCONTRADA: ${folderInfo.name}`);
            console.log(`   📅 Creada: ${folderInfo.createdTime}`);
            console.log(`   ✏️  Modificada: ${folderInfo.modifiedTime}`);
            console.log(`   📁 Tipo: ${folderInfo.mimeType}`);

            // Test 2: Listar contenido
            const listUrl = `https://www.googleapis.com/drive/v3/files?q='${folderId}'+in+parents+and+trashed=false&fields=files(id,name,mimeType,size,modifiedTime)&pageSize=5`;
            const listResponse = await fetch(listUrl, {
                headers: { Authorization: `Bearer ${token}` }
            });

            if (!listResponse.ok) {
                console.log(`   ❌ ERROR listando contenido: ${listResponse.status}`);
                return false;
            }

            const listData = await listResponse.json();
            console.log(`   📊 CONTENIDO: ${listData.files?.length || 0} items encontrados`);

            if (listData.files && listData.files.length > 0) {
                console.log(`   📋 Primeros ${Math.min(listData.files.length, 3)} items:`);
                listData.files.slice(0, 3).forEach((item, i) => {
                    const type = item.mimeType === 'application/vnd.google-apps.folder' ? '📁 CARPETA' : '📄 ARCHIVO';
                    console.log(`     ${i + 1}. ${type} - ${item.name} (${item.mimeType})`);
                    if (item.mimeType === 'application/vnd.google-apps.folder') {
                        console.log(`        ID: ${item.id}`);
                    }
                });

                // Si hay carpetas, explorar una para ver su contenido
                const firstFolder = listData.files.find(f => f.mimeType === 'application/vnd.google-apps.folder');
                if (firstFolder) {
                    console.log(`\n   🔍 Explorando subcarpeta: ${firstFolder.name}`);
                    const subListUrl = `https://www.googleapis.com/drive/v3/files?q='${firstFolder.id}'+in+parents+and+trashed=false&fields=files(id,name,mimeType)&pageSize=3`;
                    const subListResponse = await fetch(subListUrl, {
                        headers: { Authorization: `Bearer ${token}` }
                    });

                    if (subListResponse.ok) {
                        const subListData = await subListResponse.json();
                        console.log(`     📊 Contiene: ${subListData.files?.length || 0} items`);
                        if (subListData.files && subListData.files.length > 0) {
                            subListData.files.slice(0, 2).forEach((item, i) => {
                                console.log(`       ${i + 1}. ${item.name} (${item.mimeType})`);
                            });
                        }
                    }
                }
            } else {
                console.log(`   📭 La carpeta está vacía o no se pueden ver los archivos`);
            }

            return true;

        } catch (error) {
            console.log(`   🚨 EXCEPCIÓN: ${error.message}`);
            return false;
        }
    }

    async listAllItems(folderId, token) {
        const allItems = [];
        let pageToken = null;

        try {
            do {
                const url = `https://www.googleapis.com/drive/v3/files?` +
                    `q='${folderId}'+in+parents+and+trashed=false&` +
                    `fields=nextPageToken,files(id,name,mimeType,size,modifiedTime,createdTime)&` +
                    `pageSize=1000` +
                    (pageToken ? `&pageToken=${pageToken}` : '');

                const response = await fetch(url, {
                    headers: { Authorization: `Bearer ${token}` }
                });

                if (!response.ok) {
                    console.error(`   ❌ Error en listAllItems: ${response.status}`);
                    break;
                }

                const data = await response.json();
                if (data.files) {
                    allItems.push(...data.files);
                }
                pageToken = data.nextPageToken || null;

            } while (pageToken);

            return allItems;

        } catch (error) {
            console.error(`   🚨 Error en listAllItems: ${error.message}`);
            return [];
        }
    }

    async downloadFile(fileId, mimeType, token) {
        try {
            let url;
            if (mimeType.includes('application/vnd.google-apps')) {
                url = `https://www.googleapis.com/drive/v3/files/${fileId}/export?mimeType=application/pdf`;
            } else {
                url = `https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`;
            }

            const response = await fetch(url, {
                headers: { Authorization: `Bearer ${token}` }
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}`);
            }

            return await response.buffer();

        } catch (error) {
            throw new Error(`Error descargando: ${error.message}`);
        }
    }

    async uploadToGCS(destinationPath, content, contentType) {
        try {
            const file = this.storage.bucket(CONFIG.BUCKET_NAME).file(destinationPath);

            const [exists] = await file.exists();
            if (exists) {
                console.log(`      ⏭️  Ya existe: ${destinationPath}`);
                return;
            }

            await file.save(content, {
                metadata: { contentType: contentType || 'application/octet-stream' }
            });

            console.log(`      ✅ Subido: ${destinationPath}`);
            return true;

        } catch (error) {
            throw new Error(`Error subiendo: ${error.message}`);
        }
    }

    async processFolderRecursively(folderId, currentPath, token, maxDepth = 5, currentDepth = 0) {
        if (currentDepth >= maxDepth) {
            console.log(`      ⏹️  Profundidad máxima (${maxDepth}) alcanzada en: ${currentPath}`);
            return { success: 0, failed: 0 };
        }

        const stats = { success: 0, failed: 0 };
        const indent = '  '.repeat(currentDepth);

        try {
            console.log(`${indent}🔍 Nivel ${currentDepth}: Explorando ${currentPath || 'raíz'}`);

            // Listar items
            const items = await this.listAllItems(folderId, token);

            if (items.length === 0) {
                console.log(`${indent}📭 Carpeta vacía`);
                return stats;
            }

            console.log(`${indent}📊 Encontrados ${items.length} items`);

            // Separar carpetas y archivos
            const folders = items.filter(item => item.mimeType === 'application/vnd.google-apps.folder');
            const files = items.filter(item => item.mimeType !== 'application/vnd.google-apps.folder');

            console.log(`${indent}📁 Carpetas: ${folders.length}, 📄 Archivos: ${files.length}`);

            // Procesar archivos primero
            for (const file of files) {
                try {
                    console.log(`${indent}  📄 Procesando archivo: ${file.name}`);

                    const content = await this.downloadFile(file.id, file.mimeType, token);
                    const destinationPath = `${currentPath}${file.name}`;

                    await this.uploadToGCS(destinationPath, content, file.mimeType);
                    stats.success++;

                } catch (error) {
                    console.log(`${indent}  ❌ Error con ${file.name}: ${error.message}`);
                    stats.failed++;
                }
            }

            // Procesar subcarpetas recursivamente
            for (const folder of folders) {
                console.log(`${indent}  📁 Procesando subcarpeta: ${folder.name}`);

                const subStats = await this.processFolderRecursively(
                    folder.id,
                    `${currentPath}${folder.name}/`,
                    token,
                    maxDepth,
                    currentDepth + 1
                );

                stats.success += subStats.success;
                stats.failed += subStats.failed;
            }

        } catch (error) {
            console.log(`${indent}🚨 Error explorando ${currentPath}: ${error.message}`);
        }

        return stats;
    }

    async syncFolder(folder, token) {
        const { id, name } = folder;

        console.log(`\n🎯 ========================================`);
        console.log(`🎯 PROCESANDO: ${name}`);
        console.log(`🎯 ID: ${id}`);
        console.log(`🎯 ========================================`);

        // Primero, test de acceso completo
        const hasAccess = await this.testFolderAccess(id, name, token);
        if (!hasAccess) {
            console.log(`❌ SIN ACCESO a ${name}. Verifica permisos o ID.`);
            return { success: 0, failed: 0 };
        }

        // Luego, procesar recursivamente
        console.log(`\n🔄 INICIANDO PROCESAMIENTO RECURSIVO DE ${name}...`);

        try {
            const stats = await this.processFolderRecursively(id, `${name}/`, token, 10, 0);

            console.log(`\n📊 RESUMEN ${name}:`);
            console.log(`   ✅ Archivos subidos: ${stats.success}`);
            console.log(`   ❌ Archivos fallidos: ${stats.failed}`);
            console.log(`   📁 Ruta en GCS: ${CONFIG.BUCKET_NAME}/${name}/`);

            return stats;

        } catch (error) {
            console.log(`❌ Error procesando ${name}: ${error.message}`);
            return { success: 0, failed: 0 };
        }
    }

    async syncAll() {
        console.log('\n🚀 ========================================');
        console.log('🚀 INICIANDO SINCRONIZACIÓN LOGYSER');
        console.log('🚀 ========================================');

        try {
            // Inicializar si no está hecho
            if (!this.storage) {
                await this.initialize();
            }

            // Obtener token
            const token = await this.getDriveToken();

            const results = {
                timestamp: new Date().toISOString(),
                totalSuccess: 0,
                totalFailed: 0,
                folders: {}
            };

            // Procesar cada carpeta
            for (const folder of CONFIG.FOLDERS) {
                console.log(`\n\n🌟 ${'='.repeat(50)}`);
                console.log(`🌟 CARPETA: ${folder.name}`);
                console.log(`🌟 ${'='.repeat(50)}`);

                const folderStats = await this.syncFolder(folder, token);

                results.folders[folder.name] = folderStats;
                results.totalSuccess += folderStats.success;
                results.totalFailed += folderStats.failed;

                // Pequeña pausa
                await new Promise(resolve => setTimeout(resolve, 500));
            }

            // Resumen final
            console.log('\n\n🎉 ========================================');
            console.log('🎉 RESUMEN FINAL LOGYSER');
            console.log('🎉 ========================================');
            console.log(`📦 Bucket: ${CONFIG.BUCKET_NAME}`);
            console.log(`📅 Fecha: ${new Date().toLocaleString()}`);
            console.log(`\n📊 ESTADÍSTICAS:`);
            console.log(`   ✅ Total exitosos: ${results.totalSuccess}`);
            console.log(`   ❌ Total fallidos: ${results.totalFailed}`);

            console.log(`\n📁 DETALLE POR CARPETA:`);
            for (const [name, stats] of Object.entries(results.folders)) {
                console.log(`   📂 ${name}: ${stats.success} archivos sincronizados`);
            }

            return results;

        } catch (error) {
            console.error('❌ ERROR CRÍTICO:', error.message);
            console.error(error.stack);
            throw error;
        }
    }
}

// Exportar una instancia singleton
const logyserSync = new LogySerSync();
module.exports = logyserSync;
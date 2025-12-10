const { Storage } = require('@google-cloud/storage');
const { GoogleAuth } = require('google-auth-library');
const { google } = require('googleapis');
const fetch = require('node-fetch');
const express = require('express');
const { Firestore } = require('@google-cloud/firestore');
const { PubSub } = require('@google-cloud/pubsub');
const fs = require('fs');
const app = express();
const path = require('path');

// Configuración
const GOOGLE_CLOUD_PROJECT = process.env.GOOGLE_CLOUD_PROJECT || "eternal-brand-454501-i8";
const BUCKET_NAME = process.env.BUCKET_NAME || "talenthub_central";
const ROOT_FOLDER_ID = process.env.ROOT_FOLDER_ID || "1PcnN9zwjl9w_b9y99zS6gKWMhwIVdqfD";
const PORT = process.env.PORT || 8080;
const WEBHOOK_URL = process.env.WEBHOOK_URL;
const SYNC_TOPIC = process.env.SYNC_TOPIC || "drive-sync-topic";

const LOCAL_CREDENTIALS_PATH = './gcs-key.json';

console.log('🔧 Inicializando servicios de Google Cloud...');


let storage, firestore, pubsub;

// Colección para almacenar estado de sincronización
const SYNC_COLLECTION = 'drive_sync_state';
const WEBHOOK_COLLECTION = 'drive_webhooks';

// Para evitar procesamiento duplicado de notificaciones
const processedChanges = new Set();
const CHANGE_TTL = 300000; // 5 minutos

const POLLING_INTERVAL = 30000;

// Middleware
app.use(express.json());

function checkLocalCredentials() {
    const credsPath = LOCAL_CREDENTIALS_PATH;
    if (fs.existsSync(credsPath)) {
        try {
            const creds = JSON.parse(fs.readFileSync(credsPath, 'utf8'));
            console.log('✅ Credenciales locales encontradas');
            console.log(`   Tipo: ${creds.type || 'N/A'}`);
            console.log(`   Cliente ID: ${creds.client_id?.substring(0, 20)}...`);
            console.log(`   Proyecto: ${creds.project_id || 'N/A'}`);
            return true;
        } catch (error) {
            console.error('❌ Error leyendo credenciales:', error.message);
            return false;
        }
    } else {
        console.log('⚠️  No se encontró gcs-key.json en la raíz del proyecto');
        console.log('📌 El polling y sincronización manual fallarán en local');
        return false;
    }
}

function createMockFirestore() {
    console.log('🎭 Usando Firestore mock para desarrollo');

    // Cargar datos desde archivo si existe
    let mockData = {};
    const DATA_FILE = './mock-firestore-data.json';

    try {
        if (fs.existsSync(DATA_FILE)) {
            mockData = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
            console.log('📂 Datos de mock cargados desde archivo');
        }
    } catch (e) {
        console.log('📝 Creando nuevo almacenamiento mock');
    }

    // Inicializar con valores por defecto si no existen
    if (!mockData['drive_sync_state/last_sync']) {
        mockData['drive_sync_state/last_sync'] = {
            timestamp: '2000-01-01T00:00:00.000Z',
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
        };
    }

    return {
        collection: (collectionName) => ({
            doc: (docId) => ({
                get: async () => {
                    const key = `${collectionName}/${docId}`;
                    const exists = mockData[key] !== undefined;
                    console.log(`📄 Mock Firestore - GET ${key}: ${exists ? 'EXISTE' : 'NO EXISTE'}`);

                    return {
                        exists,
                        data: () => mockData[key]
                    };
                },
                set: async (data) => {
                    const key = `${collectionName}/${docId}`;
                    mockData[key] = { ...mockData[key], ...data };
                    console.log(`💾 Mock Firestore - SET ${key}:`, data.timestamp || data);

                    // Guardar en archivo para persistencia
                    try {
                        fs.writeFileSync(DATA_FILE, JSON.stringify(mockData, null, 2), 'utf8');
                    } catch (e) {
                        console.warn('⚠️  No se pudo guardar mock data:', e.message);
                    }
                }
            })
        }),
        listCollections: async () => {
            console.log('📋 Mock Firestore: Listando colecciones');
            return [];
        }
    };
}

async function startDrivePolling() {
    console.log(`🔄 Iniciando polling automático cada ${POLLING_INTERVAL / 1000} segundos...`);

    // Iniciar el primer ciclo después de 1 segundo
    setTimeout(() => {
        runPollingCycle().catch(error => {
            console.error('❌ Error fatal en startDrivePolling:', error);
        });
    }, 1000);
}

// async function initializeFirestoreWithRetry() {
//     // SI ESTAMOS EN LOCAL, USAR UN MOCK
//     if (process.env.NODE_ENV !== 'production' && !process.env.K_SERVICE) {
//         console.log('🔧 MODO DESARROLLO LOCAL - Usando mock de Firestore');
//         return createMockFirestore();
//     }

//     const maxRetries = 3;

//     for (let attempt = 1; attempt <= maxRetries; attempt++) {
//         try {
//             console.log(`Intento ${attempt}/${maxRetries} de inicializar Firestore...`);

//             const firestoreConfig = {
//                 projectId: GOOGLE_CLOUD_PROJECT,
//                 ignoreUndefinedProperties: true
//             };

//             console.log(`📁 Proyecto Firestore: ${GOOGLE_CLOUD_PROJECT}`);

//             const firestoreInstance = new Firestore(firestoreConfig);
//             await firestoreInstance.listCollections();

//             console.log('✅ Firestore inicializado correctamente');
//             return firestoreInstance;

//         } catch (error) {
//             console.error(`Intento ${attempt} fallado:`, error.message);

//             if (attempt === maxRetries) {
//                 console.error('❌ Todos los intentos fallaron.');
//                 return createMockFirestore();
//             }

//             await new Promise(resolve => setTimeout(resolve, 1000));
//         }
//     }
// }

async function initializeFirestoreWithRetry() {
    const IS_LOCAL = !process.env.K_SERVICE && process.env.NODE_ENV !== 'production';
    const KEY_FILE_PATH = path.resolve(LOCAL_CREDENTIALS_PATH);

    console.log(`🔧 Inicializando Google Cloud Storage...`);
    console.log(`   📍 Modo: ${IS_LOCAL ? 'Local' : 'Cloud'}`);
    console.log(`   📍 Ruta credenciales: ${KEY_FILE_PATH}`);
    console.log(`   📍 Existe archivo: ${fs.existsSync(KEY_FILE_PATH)}`);

    // OPCIÓN 1: Usar credenciales específicas si estamos en local
    if (IS_LOCAL && fs.existsSync(KEY_FILE_PATH)) {
        console.log('🔑 Usando credenciales locales para Storage');
        try {
            // Leer y validar credenciales
            const keyContent = JSON.parse(fs.readFileSync(KEY_FILE_PATH, 'utf8'));
            console.log(`   📧 Cuenta: ${keyContent.client_email}`);
            console.log(`   🏢 Proyecto: ${keyContent.project_id}`);

            return new Storage({
                projectId: keyContent.project_id || GOOGLE_CLOUD_PROJECT,
                keyFilename: KEY_FILE_PATH
            });
        } catch (error) {
            console.error('❌ Error con credenciales locales:', error.message);
            // Continuar con método 2
        }
    }

    // OPCIÓN 2: Usar Application Default Credentials
    console.log('🌐 Usando Application Default Credentials para Storage');
    try {
        // Configurar variable de entorno para ADC
        if (IS_LOCAL && fs.existsSync(KEY_FILE_PATH)) {
            process.env.GOOGLE_APPLICATION_CREDENTIALS = KEY_FILE_PATH;
            console.log(`   🔧 Estableciendo GOOGLE_APPLICATION_CREDENTIALS: ${KEY_FILE_PATH}`);
        }

        const storage = new Storage({
            projectId: GOOGLE_CLOUD_PROJECT
        });

        // Verificar que funciona
        const [buckets] = await storage.getBuckets();
        console.log(`✅ Storage inicializado. Buckets disponibles: ${buckets.length}`);
        return storage;

    } catch (error) {
        console.error('❌ Error con Application Default Credentials:', error.message);

        // OPCIÓN 3: Usar autenticación directa con GoogleAuth
        console.log('🔄 Intentando autenticación directa...');
        try {
            const auth = new GoogleAuth({
                keyFile: KEY_FILE_PATH,
                scopes: ['https://www.googleapis.com/auth/cloud-platform']
            });

            const client = await auth.getClient();
            const projectId = await auth.getProjectId();

            console.log(`   🔑 Autenticado como proyecto: ${projectId}`);

            return new Storage({
                projectId: projectId,
                authClient: client
            });

        } catch (authError) {
            console.error('❌ Todas las opciones de autenticación fallaron:', authError.message);
            throw new Error('No se pudo autenticar con Google Cloud Storage');
        }
    }
}

async function initializeStorageWithRetry() {
    const IS_LOCAL = !process.env.K_SERVICE && process.env.NODE_ENV !== 'production';
    const KEY_FILE_PATH = path.resolve(LOCAL_CREDENTIALS_PATH);

    console.log(`🔧 Inicializando Google Cloud Storage...`);
    console.log(`   📍 Modo: ${IS_LOCAL ? 'Local' : 'Cloud'}`);
    console.log(`   📍 Ruta credenciales: ${KEY_FILE_PATH}`);
    console.log(`   📍 Existe archivo: ${fs.existsSync(KEY_FILE_PATH)}`);

    // OPCIÓN 1: Usar credenciales específicas si estamos en local
    if (IS_LOCAL && fs.existsSync(KEY_FILE_PATH)) {
        console.log('🔑 Usando credenciales locales para Storage');
        try {
            // Leer y validar credenciales
            const keyContent = JSON.parse(fs.readFileSync(KEY_FILE_PATH, 'utf8'));
            console.log(`   📧 Cuenta: ${keyContent.client_email}`);
            console.log(`   🏢 Proyecto: ${keyContent.project_id}`);

            return new Storage({
                projectId: keyContent.project_id || GOOGLE_CLOUD_PROJECT,
                keyFilename: KEY_FILE_PATH
            });
        } catch (error) {
            console.error('❌ Error con credenciales locales:', error.message);
            // Continuar con método 2
        }
    }

    // OPCIÓN 2: Usar Application Default Credentials
    console.log('🌐 Usando Application Default Credentials para Storage');
    try {
        // Configurar variable de entorno para ADC
        if (IS_LOCAL && fs.existsSync(KEY_FILE_PATH)) {
            process.env.GOOGLE_APPLICATION_CREDENTIALS = KEY_FILE_PATH;
            console.log(`   🔧 Estableciendo GOOGLE_APPLICATION_CREDENTIALS: ${KEY_FILE_PATH}`);
        }

        const storageInstance = new Storage({
            projectId: GOOGLE_CLOUD_PROJECT
        });

        // Verificar que funciona
        const [buckets] = await storageInstance.getBuckets();
        console.log(`✅ Storage inicializado. Buckets disponibles: ${buckets.length}`);
        return storageInstance;

    } catch (error) {
        console.error('❌ Error con Application Default Credentials:', error.message);

        // OPCIÓN 3: Usar autenticación directa con GoogleAuth
        console.log('🔄 Intentando autenticación directa...');
        try {
            const auth = new GoogleAuth({
                keyFile: KEY_FILE_PATH,
                scopes: ['https://www.googleapis.com/auth/cloud-platform']
            });

            const client = await auth.getClient();
            const projectId = await auth.getProjectId();

            console.log(`   🔑 Autenticado como proyecto: ${projectId}`);

            return new Storage({
                projectId: projectId,
                authClient: client
            });

        } catch (authError) {
            console.error('❌ Todas las opciones de autenticación fallaron:', authError.message);
            throw new Error('No se pudo autenticar con Google Cloud Storage');
        }
    }
}

async function initializeGoogleCloudServices() {
    try {
        console.log('🚀 Inicializando servicios Google Cloud...');

        // 1. STORAGE - Usar función mejorada
        storage = await initializeStorageWithRetry();
        console.log('✅ Storage inicializado');

        // 2. FIRESTORE - CORREGIDO: usar la función correcta
        firestore = await initializeFirestoreWithRetry();
        console.log('✅ Firestore inicializado');

        // 3. PUBSUB - Con las mismas credenciales
        const IS_LOCAL = !process.env.K_SERVICE && process.env.NODE_ENV !== 'production';
        if (IS_LOCAL && fs.existsSync(LOCAL_CREDENTIALS_PATH)) {
            pubsub = new PubSub({
                projectId: GOOGLE_CLOUD_PROJECT,
                keyFilename: LOCAL_CREDENTIALS_PATH
            });
        } else {
            pubsub = new PubSub({ projectId: GOOGLE_CLOUD_PROJECT });
        }
        console.log('✅ PubSub inicializado');

        // Verificar conexión al bucket específico
        console.log(`🔍 Verificando acceso al bucket: ${BUCKET_NAME}`);
        try {
            const [bucketExists] = await storage.bucket(BUCKET_NAME).exists();
            if (!bucketExists) {
                console.error(`❌ ADVERTENCIA: El bucket "${BUCKET_NAME}" no existe`);
                console.log(`   Se intentará crear automáticamente si tienes permisos...`);

                try {
                    await storage.createBucket(BUCKET_NAME, {
                        location: 'us-central1',
                        storageClass: 'STANDARD'
                    });
                    console.log(`✅ Bucket "${BUCKET_NAME}" creado exitosamente`);
                } catch (createError) {
                    console.error(`❌ No se pudo crear el bucket: ${createError.message}`);
                }
            } else {
                console.log(`✅ Bucket "${BUCKET_NAME}" existe y es accesible`);

                // Listar algunos archivos para verificar permisos de escritura
                const [files] = await storage.bucket(BUCKET_NAME).getFiles({ maxResults: 3 });
                console.log(`   Archivos en bucket: ${files.length}`);
                files.forEach((file, i) => {
                    console.log(`   ${i + 1}. ${file.name}`);
                });
            }
        } catch (bucketError) {
            console.error(`❌ Error accediendo al bucket: ${bucketError.message}`);
        }

        console.log('🎉 Todos los servicios inicializados correctamente');
        return true;

    } catch (error) {
        console.error('❌ Error inicializando servicios:', error);

        // Fallback mínimo
        console.log('⚠️  Usando servicios de fallback limitados');

        // Solo inicializar storage básico
        try {
            storage = new Storage();
        } catch (e) {
            console.error('❌ No se pudo inicializar Storage de fallback');
            storage = null;
        }

        firestore = createMockFirestore();
        pubsub = { topic: () => ({ publishMessage: async () => { } }) };

        return false;
    }
}

/**
 * Obtiene último tiempo de sincronización desde Firestore
 * CON MANEJO DE ERRORES ESPECÍFICO PARA CLOUD RUN
 */
async function getLastSyncTime() {
    // VALOR POR DEFECTO - si todo falla
    const DEFAULT_TIME = '2000-01-01T00:00:00.000Z';

    try {
        // Intento 1: Obtener de Firestore normalmente
        const doc = await firestore.collection(SYNC_COLLECTION).doc('last_sync').get();

        if (doc.exists) {
            const timestamp = doc.data().timestamp;
            if (timestamp && typeof timestamp === 'string') {
                return timestamp;
            }
        }

        // Si no existe el documento, crearlo
        await firestore.collection(SYNC_COLLECTION).doc('last_sync').set({
            timestamp: DEFAULT_TIME,
            createdAt: new Date().toISOString(),
            updatedAt: new Date().toISOString()
        }, { merge: true });

        return DEFAULT_TIME;

    } catch (error) {
        // ANÁLISIS DETALLADO DEL ERROR
        console.error('🔴 ERROR CRÍTICO en getLastSyncTime:', {
            code: error.code,
            message: error.message,
            details: error.details || 'Sin detalles',
            time: new Date().toISOString()
        });

        // Si es error de Firestore, usar valor en memoria temporal
        if (error.code === 5 || error.code === 7 || error.code === 16) {
            console.log('⚠️  Usando valor de lastSyncTime en memoria');
            return DEFAULT_TIME;
        }

        throw error; // Re-lanzar otros errores
    }
}

/**
 * Guarda último tiempo de sincronización en Firestore
 */
async function setLastSyncTime(timestamp) {
    try {
        console.log(`🔄 Intentando guardar lastSyncTime: ${timestamp}`); // <-- AGREGAR
        console.log(`📝 Tipo de timestamp: ${typeof timestamp}, Valor: ${timestamp}`); // <-- AGREGAR

        await firestore.collection(SYNC_COLLECTION).doc('last_sync').set({
            timestamp: timestamp,
            updatedAt: new Date().toISOString()
        });

        console.log(`✅ lastSyncTime guardado exitosamente: ${timestamp}`); // <-- AGREGAR
    } catch (error) {
        console.error('Error guardando lastSyncTime:', error);
    }
}

/**
 * Renueva automáticamente los webhooks antes de que expiren
 */
async function renewWebhooks() {
    try {
        const snapshot = await firestore.collection(WEBHOOK_COLLECTION).get();

        for (const doc of snapshot.docs) {
            const webhookData = doc.data();
            const expirationTime = parseInt(webhookData.expiration);

            // Renovar si expira en menos de 4 horas
            if (expirationTime - Date.now() < 4 * 60 * 60 * 1000) {
                console.log('🔄 Renovando webhook que expira pronto:', webhookData.id);

                const auth = new GoogleAuth({
                    scopes: ['https://www.googleapis.com/auth/drive']
                });

                const client = await auth.getClient();
                const drive = google.drive({ version: 'v3', auth: client });

                // Obtener token de página inicial
                const startPageToken = await drive.changes.getStartPageToken();

                // Renovar webhook
                await drive.channels.stop({
                    requestBody: {
                        id: webhookData.id,
                        resourceId: webhookData.resourceId
                    }
                });

                const newWebhook = await drive.changes.watch({
                    pageToken: startPageToken.data.startPageToken,
                    requestBody: {
                        id: webhookData.id,
                        type: 'web_hook',

                        address: `${WEBHOOK_URL}/sync/webhook`,
                        expiration: (Date.now() + 86400000).toString(), // 24 horas
                    }
                });

                // Actualizar en Firestore
                await firestore.collection(WEBHOOK_COLLECTION).doc(webhookData.id).set({
                    id: newWebhook.data.id,
                    resourceId: newWebhook.data.resourceId,
                    expiration: newWebhook.data.expiration,
                    address: newWebhook.data.address,
                    updatedAt: new Date().toISOString()
                });

                console.log('✅ Webhook renovado:', newWebhook.data.id);
            }
        }
    } catch (error) {
        console.error('❌ Error renovando webhooks:', error.message);
    }
}

/**
 * Configuración inicial del webhook de Drive
 */
async function setupDriveWebhook() {
    try {
        if (!WEBHOOK_URL) {
            console.log('⚠️  WEBHOOK_URL no configurada. Solo funcionará polling');
            return;
        }

        const auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive']
        });

        const client = await auth.getClient();
        const drive = google.drive({ version: 'v3', auth: client });

        // Obtener token de página inicial
        const startPageToken = await drive.changes.getStartPageToken();
        console.log('🔑 Token de página inicial:', startPageToken.data.startPageToken);

        // Generar ID único para el webhook
        const webhookId = 'drive-to-gcs-sync-webhook-' + Date.now();

        // Configurar webhook
        const response = await drive.changes.watch({
            pageToken: startPageToken.data.startPageToken,
            requestBody: {
                id: webhookId,
                type: 'web_hook',
                address: `${WEBHOOK_URL}/sync/webhook`,
                expiration: (Date.now() + 86400000).toString(), // 24 horas
            }
        });

        // Guardar información del webhook en Firestore para renovación automática
        await firestore.collection(WEBHOOK_COLLECTION).doc(webhookId).set({
            id: response.data.id,
            resourceId: response.data.resourceId,
            expiration: response.data.expiration,
            address: response.data.address,
            createdAt: new Date().toISOString()
        });

        console.log('✅ Webhook de Drive configurado exitosamente!');
        console.log('📋 Resource ID:', response.data.resourceId);
        console.log('🌐 Drive notificará a:', WEBHOOK_URL);
        console.log('⏰ Expira:', new Date(parseInt(response.data.expiration)).toLocaleString());

    } catch (error) {
        console.error('❌ Error configurando webhook:', error.message);
        if (error.response?.data) {
            console.error('Detalles del error:', error.response.data);
        }
    }
}

/**
 * Lista archivos en carpeta con query personalizable
 */
async function listFilesInFolderREST(folderId, token, customQuery) {
    const files = [];
    let pageToken = null;
    const q = customQuery || `'${folderId}' in parents and trashed = false`;

    do {
        const url = `https://www.googleapis.com/drive/v3/files?q=${encodeURIComponent(q)}` +
            `&fields=nextPageToken,files(id,name,mimeType,modifiedTime,parents)&pageSize=1000` +
            (pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : "");

        const response = await fetch(url, {
            headers: { Authorization: "Bearer " + token },
        });

        if (!response.ok) {
            throw new Error(`Drive list error ${response.status} :: ${await response.text()}`);
        }

        const data = await response.json();
        if (data.files && data.files.length) {
            files.push(...data.files);
        }
        pageToken = data.nextPageToken || null;

    } while (pageToken);

    return files;
}

/**
 * Descarga archivo de Drive
 */
async function downloadDriveFileREST(fileId, mimeType, token) {
    let url;
    if (mimeType && mimeType.indexOf("application/vnd.google-apps") === 0) {
        url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}/export?mimeType=${encodeURIComponent("application/pdf")}`;
    } else {
        url = `https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?alt=media`;
    }

    const response = await fetch(url, {
        headers: { Authorization: "Bearer " + token },
    });

    if (!response.ok) {
        throw new Error(`Drive download error ${response.status} :: ${await response.text()}`);
    }

    const buffer = await response.buffer();
    return buffer;
}

/**
 * Sube blob a Google Cloud Storage
 */
async function uploadBlobToGCS(bucket, objectName, blob, contentType) {
    const MAX_RETRIES = 3;
    let lastError = null;

    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
            console.log(`⬆️  [Intento ${attempt}/${MAX_RETRIES}] Subiendo a GCS: ${objectName}`);

            // Verificar que storage esté inicializado
            if (!storage) {
                console.log('🔄 Re-inicializando Storage...');
                storage = await initializeStorageWithRetry();
            }

            const file = storage.bucket(bucket).file(objectName);

            // Opciones de upload
            const options = {
                metadata: {
                    contentType: contentType || 'application/octet-stream',
                },
                // Para desarrollo, podemos deshabilitar validaciones estrictas
                validation: false,
                // No usar resumable upload para archivos pequeños
                resumable: false
            };

            console.log(`   📊 Tamaño: ${blob.length} bytes`);
            console.log(`   📦 Bucket: ${bucket}`);
            console.log(`   🏷️  Content-Type: ${options.metadata.contentType}`);

            await file.save(blob, options);

            console.log(`✅ Archivo subido exitosamente: ${objectName}`);

            // Verificar que el archivo existe
            const [exists] = await file.exists();
            if (exists) {
                const [metadata] = await file.getMetadata();
                console.log(`   📅 Creado: ${metadata.timeCreated}`);
                console.log(`   🔗 URI: gs://${bucket}/${objectName}`);
            }

            return file;

        } catch (error) {
            lastError = error;
            console.error(`❌ Intento ${attempt} fallado:`, error.message);

            // Análisis específico del error
            if (error.code === 401 || error.message.includes('authentication')) {
                console.log('🔐 Error de autenticación. Re-inicializando credenciales...');
                // Forzar re-inicialización en el próximo intento
                storage = null;

                // Esperar antes de reintentar
                await new Promise(resolve => setTimeout(resolve, 2000 * attempt));

            } else if (error.code === 403) {
                console.log('🚫 Error de permisos. Verifica roles de la cuenta de servicio.');
                console.log('   La cuenta necesita: roles/storage.admin');
                break; // No reintentar errores de permisos

            } else if (error.code === 404) {
                console.log(`🔍 Bucket no encontrado: ${bucket}`);
                console.log(`   Verifica que el bucket exista en el proyecto ${GOOGLE_CLOUD_PROJECT}`);
                break; // No reintentar errores de bucket no encontrado

            } else {
                // Error genérico, esperar y reintentar
                await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
            }
        }
    }

    // Si llegamos aquí, todos los intentos fallaron
    console.error(`❌ ERROR CRÍTICO: No se pudo subir ${objectName} después de ${MAX_RETRIES} intentos`);
    console.error(`   Último error: ${lastError?.message}`);

    // Información de debug adicional
    console.log('\n🔧 INFORMACIÓN DE DEBUG:');
    console.log(`   Proyecto: ${GOOGLE_CLOUD_PROJECT}`);
    console.log(`   Bucket: ${bucket}`);
    console.log(`   Archivo: ${objectName}`);
    console.log(`   Storage inicializado: ${!!storage}`);
    console.log(`   Credenciales locales: ${fs.existsSync(LOCAL_CREDENTIALS_PATH)}`);

    if (storage) {
        try {
            const [buckets] = await storage.getBuckets();
            console.log(`   Buckets disponibles: ${buckets.map(b => b.name).join(', ')}`);
        } catch (e) {
            console.log(`   Error listando buckets: ${e.message}`);
        }
    }

    throw lastError || new Error(`Failed to upload ${objectName}`);
}

/**
 * Verifica si una carpeta está completamente vacía
 */
async function isFolderEmpty(folderId, token) {
    const q = `'${folderId}' in parents and trashed = false`;
    const items = await listFilesInFolderREST(folderId, token, q);
    return items.length === 0;
}

/**
 * Procesa carpetas recursivamente solo con archivos modificados
 */
async function processFolderIncremental(folderId, prefix, token, modifiedSince) {
    let ok = 0, fail = 0, folders = 0;

    console.log(`🔍 INICIANDO PROCESAMIENTO:`);
    console.log(`   📁 Folder ID: ${folderId}`);
    console.log(`   📍 Prefix: ${prefix || '(raíz)'}`);
    console.log(`   📅 Buscando modificados desde: ${modifiedSince}`);
    console.log(`   🔑 Token: ${token ? 'VÁLIDO' : 'INVÁLIDO'}`);

    // Construir query con validación
    const q = `'${folderId}' in parents and trashed = false and modifiedTime > '${modifiedSince}'`;
    console.log(`   🔎 Query de Drive: ${q}`);

    try {
        // 1. Obtener archivos modificados
        console.log(`   📤 Consultando Drive API...`);
        const items = await listFilesInFolderREST(folderId, token, q);
        console.log(`   📊 RESULTADO: ${items.length} items encontrados`);

        // Mostrar primeros items para debug
        if (items.length > 0) {
            console.log(`   📋 Primeros ${Math.min(items.length, 5)} items:`);
            items.slice(0, 5).forEach((item, i) => {
                const modified = new Date(item.modifiedTime).toLocaleString();
                console.log(`     ${i + 1}. ${item.name} (${item.mimeType}) - Modificado: ${modified}`);
            });
            if (items.length > 5) {
                console.log(`     ... y ${items.length - 5} más`);
            }
        }

        // 2. Si no hay items, verificar si la carpeta está vacía
        if (items.length === 0) {
            console.log(`   ℹ️  No se encontraron archivos modificados después de: ${modifiedSince}`);

            // Solo crear placeholder si la carpeta está realmente vacía
            const isEmpty = await isFolderEmpty(folderId, token);
            console.log(`   📂 La carpeta ${isEmpty ? 'ESTÁ VACÍA' : 'NO ESTÁ VACÍA, tiene archivos más antiguos'}`);

            if (isEmpty && prefix) {
                try {
                    const placeholderName = prefix + "__placeholder";
                    console.log(`   🏷️  Creando placeholder: ${placeholderName}`);
                    await uploadBlobToGCS(BUCKET_NAME, placeholderName, Buffer.from(""), "text/plain");
                    console.log(`   ✅ Placeholder creado: ${placeholderName}`);
                    ok++;
                } catch (err) {
                    console.log(`   ❌ ERROR creando placeholder: ${err.message}`);
                    fail++;
                }
            } else if (isEmpty) {
                console.log(`   ⏭️  Carpeta raíz vacía - sin placeholder`);
            }

            console.log(`   📭 FIN PROCESAMIENTO: 0 archivos procesados`);
            return { ok, fail, folders };
        }

        // 3. Procesar items encontrados
        console.log(`   🔄 Procesando ${items.length} items en: ${prefix || '(raíz)'}`);

        for (let i = 0; i < items.length; i++) {
            const item = items[i];
            console.log(`   [${i + 1}/${items.length}] Procesando: ${item.name}`);

            if (item.mimeType === "application/vnd.google-apps.folder") {
                console.log(`     📁 Es una CARPETA, procesando recursivamente...`);
                folders++;
                const subPrefix = prefix + item.name + "/";
                const subStats = await processFolderIncremental(item.id, subPrefix, token, modifiedSince);
                ok += subStats.ok;
                fail += subStats.fail;
                folders += subStats.folders;
                console.log(`     ✅ Carpeta '${item.name}' procesada: ${subStats.ok} archivos, ${subStats.folders} subcarpetas`);
            } else {
                console.log(`     📄 Es un ARCHIVO (${item.mimeType})`);
                try {
                    // Descargar archivo
                    console.log(`       ⬇️  Descargando de Drive...`);
                    const blob = await downloadDriveFileREST(item.id, item.mimeType, token);
                    console.log(`       ✅ Descargado: ${blob.length} bytes`);

                    // Subir a GCS
                    const objectName = prefix + item.name;
                    console.log(`       ⬆️  Subiendo a GCS como: ${objectName}`);
                    await uploadBlobToGCS(BUCKET_NAME, objectName, blob, item.mimeType);

                    console.log(`       ✅ SUBIDO EXITOSO: ${objectName}`);
                    ok++;

                } catch (err) {
                    console.log(`       ❌ ERROR procesando '${item.name}': ${err.message}`);

                    // Error específico para permisos
                    if (err.message.includes('403') || err.message.includes('permission')) {
                        console.log(`       🔐 Posible problema de permisos con el archivo`);
                    }
                    // Error específico para tamaño
                    else if (err.message.includes('size') || err.message.includes('large')) {
                        console.log(`       📏 Posible problema de tamaño del archivo`);
                    }

                    fail++;
                }
            }
        }

        // 4. Resumen final
        console.log(`   📈 RESUMEN PROCESAMIENTO:`);
        console.log(`     ✅ Archivos exitosos: ${ok}`);
        console.log(`     ❌ Archivos fallidos: ${fail}`);
        console.log(`     📁 Carpetas procesadas: ${folders}`);
        console.log(`     📅 Última modificación buscada: ${modifiedSince}`);

        if (ok > 0) {
            console.log(`   🎉 ¡SINCRONIZACIÓN EXITOSA!`);
        } else if (fail > 0) {
            console.log(`   ⚠️  Sin archivos exitosos, ${fail} fallos`);
        } else {
            console.log(`   ℹ️  No se procesaron archivos nuevos`);
        }

        return { ok, fail, folders };

    } catch (error) {
        console.error(`   🚨 ERROR CRÍTICO en processFolderIncremental: ${error.message}`);
        console.error(`   📍 Detalles: ${error.stack || 'Sin stack trace'}`);

        // Re-lanzar el error para manejo superior
        throw error;
    }
}

/**
 * Procesa cambios en tiempo real con manejo de duplicados
 */
async function processRealTimeChange(changeId, fileId, resourceState) {
    // Evitar procesamiento duplicado
    if (processedChanges.has(changeId)) {
        console.log('⏭️  Cambio ya procesado:', changeId);
        return;
    }

    // Agregar a procesados con TTL
    processedChanges.add(changeId);
    setTimeout(() => processedChanges.delete(changeId), CHANGE_TTL);

    try {
        const auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive']
        });

        const client = await auth.getClient();
        const token = (await client.getAccessToken()).token;

        // Obtener información del archivo modificado
        const driveResponse = await fetch(
            `https://www.googleapis.com/drive/v3/files/${fileId}?fields=id,name,mimeType,modifiedTime,parents,trashed`,
            { headers: { Authorization: `Bearer ${token}` } }
        );

        if (!driveResponse.ok) {
            throw new Error(`Error obteniendo información del archivo: ${driveResponse.status}`);
        }

        const file = await driveResponse.json();

        // Si el archivo está en papelera, eliminarlo de GCS
        if (file.trashed) {
            try {
                // Intentar eliminar de GCS
                const fileName = file.name;
                await storage.bucket(BUCKET_NAME).file(fileName).delete();
                console.log(`🗑️  Archivo eliminado de GCS: ${fileName}`);
            } catch (deleteError) {
                if (deleteError.code === 404) {
                    console.log(`⚠️  Archivo no encontrado en GCS para eliminar: ${file.name}`);
                } else {
                    throw deleteError;
                }
            }
            return;
        }

        console.log(`📤 Sincronizando: ${file.name} (${resourceState})`);

        // Si es una carpeta, procesar recursivamente
        if (file.mimeType === "application/vnd.google-apps.folder") {
            await processFolderIncremental(file.id, file.name + "/", token, new Date(0).toISOString());
        } else {
            // Descargar y subir el archivo
            const blob = await downloadDriveFileREST(file.id, file.mimeType, token);
            await uploadBlobToGCS(BUCKET_NAME, file.name, blob, file.mimeType);
            console.log(`✅ Sincronizado en tiempo real: ${file.name}`);
        }

    } catch (error) {
        console.error('❌ Error procesando cambio en tiempo real:', error);

        // Reintentar después de un delay usando Pub/Sub para mejor escalabilidad
        await pubsub.topic(SYNC_TOPIC).publishMessage({
            data: Buffer.from(JSON.stringify({
                changeId: changeId,
                fileId: fileId,
                resourceState: resourceState,
                retryCount: 1
            }))
        });
    }
}

/**
 * Obtiene último page token de sincronización desde Firestore
 */
async function getLastSyncPageToken() {
    try {
        const doc = await firestore.collection(SYNC_COLLECTION).doc('page_token').get();
        if (doc.exists) {
            return doc.data().token;
        }

        // Si no existe, obtener uno nuevo
        const auth = new GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/drive']
        });
        const client = await auth.getClient();
        const drive = google.drive({ version: 'v3', auth: client });
        const startPageToken = await drive.changes.getStartPageToken();

        await setLastSyncPageToken(startPageToken.data.startPageToken);
        return startPageToken.data.startPageToken;
    } catch (error) {
        console.error('Error obteniendo page token:', error);
        throw error;
    }
}

/**
 * Guarda último page token de sincronización en Firestore
 */
async function setLastSyncPageToken(token) {
    try {
        await firestore.collection(SYNC_COLLECTION).doc('page_token').set({
            token: token,
            updatedAt: new Date().toISOString()
        });
    } catch (error) {
        console.error('Error guardando page token:', error);
        throw error;
    }
}

/**
 * Webhook mejorado para notificaciones en tiempo real de Drive
 */
app.post('/sync/webhook', async (req, res) => {
    console.log('📩 Notificación de Drive recibida!');

    // Verificar que es una notificación válida de Drive
    const resourceId = req.headers['x-goog-resource-id'];
    const resourceState = req.headers['x-goog-resource-state'];
    const resourceUri = req.headers['x-goog-resource-uri'];
    const channelId = req.headers['x-goog-channel-id'];

    if (!resourceId || !resourceState) {
        console.log('⚠️  Notificación inválida, faltan headers necesarios');
        return res.status(400).send('Notificación inválida');
    }

    // Generar ID único para este cambio
    const changeId = `${resourceId}-${Date.now()}`;

    // Responder inmediatamente (Drive requiere respuesta rápida)
    res.status(200).send('✅ Notificación recibida');

    // Procesar en segundo plano
    setTimeout(async () => {
        try {
            console.log(`🔄 Procesando cambio: ${resourceState} para resource: ${resourceId}`);

            // Para cambios, necesitamos obtener los archivos modificados
            if (resourceState === 'change' || resourceState === 'update' || resourceState === 'add') {
                const auth = new GoogleAuth({
                    scopes: ['https://www.googleapis.com/auth/drive']
                });

                const client = await auth.getClient();
                const drive = google.drive({ version: 'v3', auth: client });

                // Obtener el page token actual
                const pageToken = await getLastSyncPageToken();

                // Obtener los cambios recientes
                const changes = await drive.changes.list({
                    pageToken: pageToken,
                    pageSize: 10
                });

                // Procesar cada cambio
                for (const change of changes.data.changes) {
                    if (change.fileId) {
                        await processRealTimeChange(
                            `${change.fileId}-${Date.now()}`,
                            change.fileId,
                            resourceState
                        );
                    }
                }

                // Actualizar el page token
                if (changes.data.newStartPageToken) {
                    await setLastSyncPageToken(changes.data.newStartPageToken);
                }
            }
        } catch (error) {
            console.error('❌ Error procesando webhook:', error);
        }
    }, 1000);
});

/**
 * Ruta principal que Cloud Run health check requiere
 */
// Endpoint raíz para health checks - VERSIÓN MEJORADA
app.get('/', (req, res) => {
    const currentTime = new Date().toISOString();
    const formattedTime = new Date().toLocaleString('es-ES', {
        weekday: 'long',
        year: 'numeric',
        month: 'long',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        timeZone: 'America/Bogota'
    });

    const html = `
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TalentHub Sync Service</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            display: flex;
            justify-content: center;
            align-items: center;
            color: #333;
        }

        .container {
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
            width: 90%;
            max-width: 1200px;
            overflow: hidden;
            margin: 20px;
        }

        .header {
            background: linear-gradient(135deg, #1a237e 0%, #283593 100%);
            color: white;
            padding: 40px;
            text-align: center;
            position: relative;
        }

        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, #00bcd4, #4caf50);
        }

        .logo {
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 15px;
            margin-bottom: 20px;
        }

        .logo-icon {
            font-size: 48px;
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { transform: scale(1); }
            50% { transform: scale(1.1); }
            100% { transform: scale(1); }
        }

        h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
            font-weight: 600;
        }

        .tagline {
            font-size: 1.2rem;
            opacity: 0.9;
            margin-bottom: 30px;
        }

        .status-badge {
            display: inline-block;
            background: #4caf50;
            color: white;
            padding: 8px 24px;
            border-radius: 50px;
            font-weight: 600;
            font-size: 1.1rem;
            box-shadow: 0 4px 15px rgba(76, 175, 80, 0.3);
        }

        .content {
            padding: 40px;
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 30px;
            margin-bottom: 40px;
        }

        .card {
            background: #f8f9fa;
            border-radius: 15px;
            padding: 25px;
            border-left: 4px solid #1a237e;
            transition: transform 0.3s ease, box-shadow 0.3s ease;
        }

        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.1);
        }

        .card h3 {
            color: #1a237e;
            margin-bottom: 15px;
            font-size: 1.3rem;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .card-icon {
            font-size: 24px;
        }

        .info-item {
            margin-bottom: 12px;
            padding-bottom: 12px;
            border-bottom: 1px solid #e0e0e0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .info-label {
            font-weight: 600;
            color: #555;
        }

        .info-value {
            color: #333;
            font-family: 'Courier New', monospace;
        }

        .endpoints {
            background: #1a237e;
            color: white;
            padding: 30px;
            border-radius: 15px;
            margin-top: 30px;
        }

        .endpoints h3 {
            margin-bottom: 20px;
            font-size: 1.4rem;
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .endpoint-item {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 10px;
            padding: 15px;
            margin-bottom: 15px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            transition: background 0.3s ease;
        }

        .endpoint-item:hover {
            background: rgba(255, 255, 255, 0.2);
        }

        .method {
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: 600;
            font-size: 0.9rem;
        }

        .method.get { background: #4caf50; color: white; }
        .method.post { background: #ff9800; color: white; }
        .path { font-family: 'Courier New', monospace; }

        .footer {
            text-align: center;
            padding: 30px;
            color: #666;
            border-top: 1px solid #e0e0e0;
            margin-top: 40px;
        }

        .uptime {
            display: inline-block;
            background: #e3f2fd;
            color: #1a237e;
            padding: 10px 20px;
            border-radius: 10px;
            font-weight: 600;
            margin-top: 15px;
        }

        @media (max-width: 768px) {
            .container {
                margin: 10px;
                width: 95%;
            }
            
            .header {
                padding: 30px 20px;
            }
            
            .content {
                padding: 20px;
            }
            
            .grid {
                grid-template-columns: 1fr;
            }
            
            h1 {
                font-size: 2rem;
            }
        }
    </style>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="logo">
                <div class="logo-icon">🔄</div>
                <div>
                    <h1>TalentHub Sync Service</h1>
                    <div class="tagline">Sincronización automatizada Drive → Google Cloud Storage</div>
                </div>
            </div>
            <div class="status-badge">
                <i class="fas fa-check-circle"></i> Servicio Activo
            </div>
        </div>
        
        <div class="content">
            <div class="grid">
                <div class="card">
                    <h3><i class="fas fa-info-circle card-icon"></i> Información del Servicio</h3>
                    <div class="info-item">
                        <span class="info-label">Estado:</span>
                        <span class="info-value" style="color: #4caf50; font-weight: 600;">
                            <i class="fas fa-check-circle"></i> Operacional
                        </span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Versión:</span>
                        <span class="info-value">1.0.0</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Última actualización:</span>
                        <span class="info-value">${formattedTime}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Entorno:</span>
                        <span class="info-value">${process.env.NODE_ENV || 'development'}</span>
                    </div>
                </div>
                
                <div class="card">
                    <h3><i class="fas fa-database card-icon"></i> Configuración</h3>
                    <div class="info-item">
                        <span class="info-label">Proyecto GCP:</span>
                        <span class="info-value">${GOOGLE_CLOUD_PROJECT}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Bucket GCS:</span>
                        <span class="info-value">${BUCKET_NAME}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Carpeta Drive:</span>
                        <span class="info-value">${ROOT_FOLDER_ID}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Puerto:</span>
                        <span class="info-value">${PORT}</span>
                    </div>
                </div>
                
                <div class="card">
                    <h3><i class="fas fa-chart-line card-icon"></i> Estadísticas</h3>
                    <div class="info-item">
                        <span class="info-label">Modo sincronización:</span>
                        <span class="info-value">${WEBHOOK_URL ? 'Webhook + Polling' : 'Polling'}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Intervalo polling:</span>
                        <span class="info-value">${POLLING_INTERVAL / 1000} segundos</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Sincronización:</span>
                        <span class="info-value">${WEBHOOK_URL ? '<span style="color:#4caf50;">Tiempo real</span>' : '<span style="color:#ff9800;">Programada</span>'}</span>
                    </div>
                    <div class="info-item">
                        <span class="info-label">Monitoreo:</span>
                        <span class="info-value"><span style="color:#4caf50;">Activo</span></span>
                    </div>
                </div>
            </div>
            
            <div class="endpoints">
                <h3><i class="fas fa-plug card-icon"></i> Endpoints Disponibles</h3>
                
                <div class="endpoint-item">
                    <div>
                        <span class="method get">GET</span>
                        <span class="path">/</span>
                    </div>
                    <span>Health Check (esta página)</span>
                </div>
                
                <div class="endpoint-item">
                    <div>
                        <span class="method post">POST</span>
                        <span class="path">/sync</span>
                    </div>
                    <span>Sincronización manual Drive → GCS</span>
                </div>
                
                <div class="endpoint-item">
                    <div>
                        <span class="method get">GET</span>
                        <span class="path">/debug/storage</span>
                    </div>
                    <span>Diagnóstico de Google Cloud Storage</span>
                </div>
                
                ${WEBHOOK_URL ? `
                <div class="endpoint-item">
                    <div>
                        <span class="method post">POST</span>
                        <span class="path">/sync/webhook</span>
                    </div>
                    <span>Webhook Drive (sincronización tiempo real)</span>
                </div>
                ` : ''}
                
                <div class="endpoint-item">
                    <div>
                        <span class="method get">GET</span>
                        <span class="path">/sync/scheduled</span>
                    </div>
                    <span>Health Check programado</span>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>TalentHub Sync Service © ${new Date().getFullYear()} - Sistema de sincronización automatizada</p>
            <p class="uptime">
                <i class="fas fa-clock"></i> Último check: ${new Date().toLocaleTimeString('es-ES')}
            </p>
            <p style="margin-top: 15px; font-size: 0.9rem;">
                <i class="fas fa-shield-alt"></i> Servicio seguro | 
                <i class="fas fa-bolt"></i> Alta disponibilidad | 
                <i class="fas fa-sync-alt"></i> Sincronización continua
            </p>
        </div>
    </div>
    
    <script>
        // Actualizar la hora automáticamente cada minuto
        function updateTime() {
            const now = new Date();
            const timeElement = document.querySelector('.uptime');
            if (timeElement) {
                timeElement.innerHTML = '<i class="fas fa-clock"></i> Último check: ' + 
                    now.toLocaleTimeString('es-ES', { 
                        hour: '2-digit', 
                        minute: '2-digit',
                        second: '2-digit'
                    });
            }
        }
        
        // Actualizar cada 60 segundos
        setInterval(updateTime, 60000);
        
        // Efecto de carga suave
        document.addEventListener('DOMContentLoaded', function() {
            const cards = document.querySelectorAll('.card');
            cards.forEach((card, index) => {
                card.style.opacity = '0';
                card.style.transform = 'translateY(20px)';
                setTimeout(() => {
                    card.style.transition = 'opacity 0.5s ease, transform 0.5s ease';
                    card.style.opacity = '1';
                    card.style.transform = 'translateY(0)';
                }, index * 100);
            });
            
            // Efecto de ping para el estado
            const statusBadge = document.querySelector('.status-badge');
            if (statusBadge) {
                setInterval(() => {
                    statusBadge.style.transform = 'scale(1.05)';
                    setTimeout(() => {
                        statusBadge.style.transform = 'scale(1)';
                    }, 300);
                }, 3000);
            }
        });
    </script>
</body>
</html>
    `;

    res.status(200).send(html);
});

app.get('/debug/storage', async (req, res) => {
    try {
        if (!storage) {
            return res.status(500).json({ error: 'Storage no inicializado' });
        }

        // 1. Obtener información del proyecto
        const projectId = storage.projectId;
        const authClient = storage.authClient;

        // 2. Listar buckets
        const [buckets] = await storage.getBuckets();

        // 3. Verificar bucket específico
        const bucket = storage.bucket(BUCKET_NAME);
        const [bucketExists] = await bucket.exists();

        let bucketInfo = { exists: bucketExists };
        if (bucketExists) {
            const [metadata] = await bucket.getMetadata();
            const [files] = await bucket.getFiles({ maxResults: 5 });

            bucketInfo = {
                ...bucketInfo,
                metadata: {
                    name: metadata.name,
                    location: metadata.location,
                    storageClass: metadata.storageClass,
                    timeCreated: metadata.timeCreated
                },
                files: files.map(f => ({
                    name: f.name,
                    size: f.metadata.size,
                    contentType: f.metadata.contentType
                }))
            };
        }

        // 4. Verificar credenciales
        const credentialsInfo = {
            localFile: {
                path: LOCAL_CREDENTIALS_PATH,
                exists: fs.existsSync(LOCAL_CREDENTIALS_PATH)
            },
            env: {
                GOOGLE_APPLICATION_CREDENTIALS: process.env.GOOGLE_APPLICATION_CREDENTIALS,
                GOOGLE_CLOUD_PROJECT: process.env.GOOGLE_CLOUD_PROJECT
            }
        };

        if (fs.existsSync(LOCAL_CREDENTIALS_PATH)) {
            try {
                const keyContent = JSON.parse(fs.readFileSync(LOCAL_CREDENTIALS_PATH, 'utf8'));
                credentialsInfo.serviceAccount = {
                    clientEmail: keyContent.client_email,
                    projectId: keyContent.project_id,
                    privateKeyId: keyContent.private_key_id
                };
            } catch (e) {
                credentialsInfo.fileError = e.message;
            }
        }

        res.json({
            timestamp: new Date().toISOString(),
            project: projectId,
            storageInitialized: !!storage,
            credentials: credentialsInfo,
            buckets: buckets.map(b => b.name),
            targetBucket: bucketInfo
        });

    } catch (error) {
        res.status(500).json({
            error: error.message,
            stack: error.stack,
            code: error.code,
            details: error.details
        });
    }
});

/**
 * Ruta para ejecutar la sincronización manualmente
 */
app.post('/sync', async (req, res) => {
    console.log("🔍 Iniciando sincronización manual de Drive a GCS");

    try {
        const IS_LOCAL = !process.env.K_SERVICE && process.env.NODE_ENV !== 'production';
        const HAS_LOCAL_CREDS = require('fs').existsSync('./gcs-key.json');

        let auth;

        if (IS_LOCAL && HAS_LOCAL_CREDS) {
            console.log('🔑 Sincronización manual con credenciales locales');
            auth = new GoogleAuth({
                keyFile: './gcs-key.json',
                scopes: ['https://www.googleapis.com/auth/drive']
            });
        } else if (IS_LOCAL) {
            return res.status(400).json({
                status: 'error',
                message: 'No se encontraron credenciales locales. Crea un archivo gcs-key.json'
            });
        } else {
            auth = new GoogleAuth({
                scopes: ['https://www.googleapis.com/auth/drive']
            });
        }

        const client = await auth.getClient();
        const token = (await client.getAccessToken()).token;

        const lastSyncTime = await getLastSyncTime();
        const currentTime = new Date().toISOString();

        console.log("Buscando archivos modificados desde: " + lastSyncTime);

        const stats = await processFolderIncremental(ROOT_FOLDER_ID, "", token, lastSyncTime);
        await setLastSyncTime(currentTime);

        console.log(`✅ Sincronización manual completada. 
Archivos: ${stats.ok} 
Fallidos: ${stats.fail} 
Carpetas: ${stats.folders}`);

        res.status(200).json({
            status: 'success',
            message: 'Sincronización completada',
            stats: stats
        });

    } catch (error) {
        console.error("❌ Error en sincronización manual:", error.message);

        // Error específico para credenciales inválidas
        if (error.message.includes('invalid_grant')) {
            return res.status(401).json({
                status: 'error',
                message: 'Credenciales OAuth inválidas o expiradas. Regenera el archivo gcs-key.json'
            });
        }

        res.status(500).json({
            status: 'error',
            message: error.message
        });
    }
});

async function runPollingCycle() {
    // Variable para detectar si es la primera sincronización
    let isFirstRun = true;

    // Función interna recursiva
    async function executePolling() {
        try {
            console.log('⏰ Ciclo de polling iniciado...');

            const IS_LOCAL = !process.env.K_SERVICE;
            const HAS_LOCAL_CREDS = require('fs').existsSync(LOCAL_CREDENTIALS_PATH);

            // Obtener lastSyncTime
            let lastRun;
            try {
                lastRun = await getLastSyncTime();
                console.log(`📅 Última sincronización: ${lastRun}`);
            } catch (syncError) {
                console.warn('⚠️  No se pudo obtener lastSyncTime:', syncError.message);
                lastRun = '2000-01-01T00:00:00.000Z';
            }

            // DETECTAR SI ES LA PRIMERA EJECUCIÓN DESPUÉS DEL INICIO
            if (isFirstRun && lastRun === '2000-01-01T00:00:00.000Z') {
                console.log('🚀 ¡PRIMERA SINCRONIZACIÓN DETECTADA!');
                console.log('📥 Obteniendo TODOS los archivos desde el inicio...');
                isFirstRun = false;
            }

            // AUTENTICACIÓN
            let auth;
            if (IS_LOCAL && HAS_LOCAL_CREDS) {
                console.log('🔑 Usando credenciales locales');
                auth = new GoogleAuth({
                    keyFile: LOCAL_CREDENTIALS_PATH,
                    scopes: ['https://www.googleapis.com/auth/drive']
                });
            } else if (IS_LOCAL) {
                console.error('❌ No se encontraron credenciales locales');
                setTimeout(executePolling, POLLING_INTERVAL);
                return;
            } else {
                auth = new GoogleAuth({
                    scopes: ['https://www.googleapis.com/auth/drive']
                });
            }

            const client = await auth.getClient();
            const token = (await client.getAccessToken()).token;

            // DECIDIR QUÉ FECHA USAR
            let modifiedSince;

            if (lastRun === '2000-01-01T00:00:00.000Z') {
                // PRIMERA VEZ: obtener TODO
                modifiedSince = '2000-01-01T00:00:00.000Z';
                console.log('📅 Buscando TODOS los archivos (primera sincronización)');
            } else {
                // Incremental: buscar desde lastRun o últimos 5 minutos
                const fiveMinutesAgo = new Date(Date.now() - 5 * 60000).toISOString();
                modifiedSince = lastRun < fiveMinutesAgo ? fiveMinutesAgo : lastRun;
                console.log(`📅 Buscando cambios desde: ${modifiedSince}`);
            }

            // EJECUTAR SINCRONIZACIÓN
            console.log(`🔍 Consultando Drive...`);
            const stats = await processFolderIncremental(ROOT_FOLDER_ID, "", token, modifiedSince);

            // ACTUALIZAR lastSyncTime SIEMPRE
            const newSyncTime = new Date().toISOString();
            try {
                await setLastSyncTime(newSyncTime);
                console.log(`📝 lastSyncTime actualizado a: ${newSyncTime}`);
            } catch (updateError) {
                console.warn('⚠️  No se pudo actualizar lastSyncTime:', updateError.message);
            }

            // MOSTRAR RESULTADOS
            if (stats.ok > 0) {
                console.log(`✅ ${stats.ok} archivos sincronizados, ${stats.fail} fallos`);
            } else if (stats.fail > 0) {
                console.log(`❌ ${stats.fail} archivos fallaron`);
            } else {
                console.log('✅ No hay cambios nuevos');
            }

        } catch (error) {
            console.error('❌ Error en ciclo de polling:', error.message);
        } finally {
            // Programar próximo ciclo
            setTimeout(executePolling, POLLING_INTERVAL);
        }
    }

    // Iniciar el ciclo
    await executePolling();
}

// Agrega este endpoint para los health checks de Google
app.post('/sync/scheduled', (req, res) => {
    console.log('✅ Health check recibido de Google');
    res.status(200).json({
        status: 'ok',
        message: 'Service is running',
        timestamp: new Date().toISOString()
    });
});

// También agrega un endpoint GET para health checks
app.get('/sync/scheduled', (req, res) => {
    console.log('✅ Health check GET recibido');
    res.status(200).json({
        status: 'ok',
        message: 'Service is healthy',
        timestamp: new Date().toISOString()
    });
});

// Iniciar servidor
app.listen(PORT, async () => {
    console.log(`🚀 Servidor ejecutándose en puerto ${PORT}`);
    console.log(`📁 Ruta actual: ${__dirname}`);
    console.log(`📁 Ruta credenciales: ${path.resolve(LOCAL_CREDENTIALS_PATH)}`);

    const IS_LOCAL = !process.env.K_SERVICE && process.env.NODE_ENV !== 'production';

    if (IS_LOCAL) {
        const hasCreds = checkLocalCredentials();
        if (!hasCreds) {
            console.error('❌ CRÍTICO: No hay credenciales locales');
            console.log('   Crea un archivo gcs-key.json o establece GOOGLE_APPLICATION_CREDENTIALS');
        }
    }

    try {
        // Inicializar servicios
        await initializeGoogleCloudServices();

        // Iniciar servicios adicionales
        firestore = await initializeFirestoreWithRetry();

        if (WEBHOOK_URL) {
            await setupDriveWebhook();
        }

        if (firestore) {
            startDrivePolling();
        }

        console.log('✅ Servicio listo');
        console.log(`📌 Debug endpoint: GET http://localhost:${PORT}/debug/storage`);

    } catch (error) {
        console.error('❌ Error durante inicialización:', error);
    }
});

module.exports = { app };
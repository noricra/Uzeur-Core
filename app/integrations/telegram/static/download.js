// Initialize Telegram WebApp
const tg = window.Telegram.WebApp;
tg.expand();

// Get parameters from URL
const urlParams = new URLSearchParams(window.location.search);
const productId = urlParams.get('product_id');
const userLang = urlParams.get('lang') || 'fr';

// Translations
const translations = {
    fr: {
        notInTelegram: 'Cette application doit être ouverte depuis Telegram.',
        useButton: 'Utilisez le bouton "📥 Télécharger" dans le bot.',
        error: 'Erreur',
        verifying: 'Vérification de votre achat...',
        notPurchased: 'Vous n\'avez pas acheté ce produit.',
        fileNotAvailable: 'Fichier non disponible.',
        downloadError: 'Erreur lors du téléchargement',
        networkError: 'Erreur réseau. Vérifiez votre connexion.',
        unknownError: 'Une erreur inconnue est survenue.'
    },
    en: {
        notInTelegram: 'This application must be opened from Telegram.',
        useButton: 'Use the "📥 Download" button in the bot.',
        error: 'Error',
        verifying: 'Verifying your purchase...',
        notPurchased: 'You have not purchased this product.',
        fileNotAvailable: 'File not available.',
        downloadError: 'Download error',
        networkError: 'Network error. Check your connection.',
        unknownError: 'An unknown error occurred.'
    }
};

// Translation helper
const t = (key) => translations[userLang][key] || translations['fr'][key];

// Vérifier que l'app est bien dans Telegram
if (!tg.initData || tg.initData.length === 0) {
    console.error('Not running in Telegram WebApp or initData is empty');
    document.body.innerHTML = `
        <div style="padding: 20px; text-align: center;">
            <h2>${t('error')}</h2>
            <p>${t('notInTelegram')}</p>
            <p>${t('useButton')}</p>
        </div>
    `;
    throw new Error('Not in Telegram WebApp');
}

// Get user data from Telegram
const userId = tg.initDataUnsafe?.user?.id;
const username = tg.initDataUnsafe?.user?.username;

// Log for debugging
console.log('Telegram WebApp initialized');
console.log('User ID:', userId);
console.log('Product ID:', productId);

// Global variables
let purchaseData = null;

// DOM Elements (will be set after DOM loads)
let loadingSection, productSection, progressSection, successSection, errorSection;
let productTitle, productSize, downloadCount, downloadBtn;
let progressBar, progressPercent, downloadSpeed, fileName, downloadedSize, totalSize;
let successFileName, errorMessage;

// Initialize DOM elements
function initDOMElements() {
    loadingSection = document.getElementById('loadingSection');
    productSection = document.getElementById('productSection');
    progressSection = document.getElementById('progressSection');
    successSection = document.getElementById('successSection');
    errorSection = document.getElementById('errorSection');

    productTitle = document.getElementById('productTitle');
    productSize = document.getElementById('productSize');
    downloadCount = document.getElementById('downloadCount');
    downloadBtn = document.getElementById('downloadBtn');

    progressBar = document.getElementById('progressBar');
    progressPercent = document.getElementById('progressPercent');
    downloadSpeed = document.getElementById('downloadSpeed');
    fileName = document.getElementById('fileName');
    downloadedSize = document.getElementById('downloadedSize');
    totalSize = document.getElementById('totalSize');

    successFileName = document.getElementById('successFileName');
    errorMessage = document.getElementById('errorMessage');
}

// Show/hide sections helper
function showSection(sectionId) {
    const sections = ['loadingSection', 'productSection', 'progressSection', 'successSection', 'errorSection'];
    sections.forEach(id => {
        const element = document.getElementById(id);
        if (element) {
            if (id === sectionId) {
                element.classList.remove('hidden');
            } else {
                element.classList.add('hidden');
            }
        }
    });
}

// Show error
function showError(message) {
    errorMessage.textContent = message;
    showSection('errorSection');
}

// Format file size
function formatFileSize(mb) {
    if (mb < 1) {
        return `${(mb * 1024).toFixed(2)} KB`;
    } else if (mb < 1024) {
        return `${mb.toFixed(2)} MB`;
    } else {
        return `${(mb / 1024).toFixed(2)} GB`;
    }
}

// Verify purchase on page load
async function verifyPurchase() {
    if (!productId) {
        console.error('❌ [VERIFY] Product ID manquant dans l\'URL');
        showError('Product ID manquant dans l\'URL');
        return;
    }

    try {
        const requestBody = {
            product_id: productId,
            user_id: userId,
            telegram_init_data: tg.initData
        };
        console.log('🔍 [VERIFY] Starting verification with params:', {
            product_id: productId,
            user_id: userId,
            initData_length: tg.initData?.length || 0
        });

        const response = await fetch('/api/verify-purchase', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(requestBody)
        });

        console.log(`📡 [VERIFY] Response status: ${response.status} ${response.statusText}`);

        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            const errorDetail = errorData.detail || `HTTP ${response.status}`;
            console.error('❌ [VERIFY] API Error:', {
                status: response.status,
                statusText: response.statusText,
                detail: errorDetail,
                fullError: errorData
            });

            if (response.status === 404) {
                showError(t('notPurchased'));
                return;
            } else if (response.status === 401) {
                showError('Authentification échouée: ' + errorDetail);
                return;
            }
            showError(`Erreur ${response.status}: ${errorDetail}`);
            return;
        }

        const data = await response.json();
        console.log('✅ [VERIFY] Purchase verified successfully:', data);
        purchaseData = data;

        // Display product info
        productTitle.textContent = data.product_title;
        productSize.textContent = `Taille: ${formatFileSize(data.file_size_mb)}`;

        const downloadCountText = userLang === 'fr'
            ? `Téléchargé ${data.download_count} fois`
            : `Downloaded ${data.download_count} times`;
        downloadCount.textContent = downloadCountText;

        // Check if file is available
        if (!data.has_file) {
            showError(t('fileNotAvailable'));
            return;
        }

        // Show product section
        showSection('productSection');

    } catch (error) {
        console.error('Error verifying purchase:', error);
        showError(t('networkError'));
    }
}

// Handle download button click
function setupDownloadButton() {
    downloadBtn.addEventListener('click', async () => {
        if (!purchaseData) {
            console.error('[DOWNLOAD] Purchase data not available');
            showError('Purchase data not available');
            return;
        }

        try {
            console.error('========== NATIVE BROWSER DOWNLOAD (NO BLOB) ==========');
            console.log('[DOWNLOAD] Generating download token...', {
                product_id: purchaseData.product_id,
                order_id: purchaseData.order_id,
                user_id: userId
            });

            // Step 1: Generate one-time token
            const tokenResponse = await fetch('/api/generate-download-token', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    product_id: purchaseData.product_id,
                    order_id: purchaseData.order_id,
                    user_id: userId,
                    telegram_init_data: tg.initData
                })
            });

            if (!tokenResponse.ok) {
                throw new Error('Failed to generate download token');
            }

            const { download_token } = await tokenResponse.json();
            console.error('[DOWNLOAD] Token generated:', download_token);

            // Step 2: Open download URL via Telegram API (works better in WebView)
            const downloadUrl = `${window.location.origin}/download/${download_token}`;
            console.error('[DOWNLOAD] Opening download URL:', downloadUrl);

            // Try Telegram openLink API (better for WebView)
            if (typeof tg !== 'undefined' && tg.openLink) {
                console.error('[DOWNLOAD] Using tg.openLink()');
                tg.openLink(downloadUrl);
            } else {
                console.error('[DOWNLOAD] Fallback to window.open()');
                window.open(downloadUrl, '_blank');
            }

            // Show success message after short delay
            setTimeout(() => {
                successFileName.textContent = purchaseData.product_title;
                showSection('successSection');
            }, 1000);

        } catch (error) {
            console.error('[DOWNLOAD] Error:', error);
            showError(t('downloadError'));
        }
    });
}

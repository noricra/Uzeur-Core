"""
IPN Server avec support Webhook Telegram et Mini App Auth (Corrigé)
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import hmac
import hashlib
import json
import os
import logging
import asyncio
import sys
import uuid
# IMPORT CRITIQUE POUR LE FIX 401
from urllib.parse import parse_qsl

# Gestion du path pour les imports relatifs
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from telegram import Bot, Update
from telegram.ext import Application

from app.core import settings as core_settings
from app.core.database_init import get_postgresql_connection
from app.core.db_pool import put_connection
from app.core.file_utils import get_b2_presigned_url
from app.services.b2_storage_service import B2StorageService
from app.domain.repositories.order_repo import OrderRepository
from app.domain.repositories.download_repo import DownloadRepository
from app.services.seller_payout_service import SellerPayoutService

# --- IMPORTS DU BOT ---
from app.integrations.telegram.app_builder import build_application
from bot_mlt import MarketplaceBot

# Configuration Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Variable globale pour l'application Telegram
telegram_application: Optional[Application] = None

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 1. LIFESPAN (Démarrage/Arrêt)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestionnaire de cycle de vie.
    Initialise le bot Telegram AVANT que le serveur n'accepte des requêtes.
    """
    global telegram_application

    logger.info("🚀 Initialisation du Bot Telegram dans le lifespan...")

    if not core_settings.TELEGRAM_BOT_TOKEN:
        logger.error("❌ TELEGRAM_BOT_TOKEN manquant !")
    else:
        try:
            # 1. Créer le bot et l'application
            bot_instance = MarketplaceBot()
            telegram_application = build_application(bot_instance)
            bot_instance.application = telegram_application

            # 2. Initialiser explicitement
            await telegram_application.initialize()
            await telegram_application.start()

            # 3. Configurer Webhook OU Polling
            webhook_url = core_settings.WEBHOOK_URL
            # On active le webhook seulement si c'est une URL https distante (pas localhost)
            use_webhook = webhook_url and 'localhost' not in webhook_url and webhook_url.startswith('https')

            if use_webhook:
                webhook_full_url = f"{webhook_url}/webhook/telegram"
                await telegram_application.bot.set_webhook(
                    url=webhook_full_url,
                    drop_pending_updates=True,
                    allowed_updates=Update.ALL_TYPES
                )
                logger.info(f"✅ Telegram webhook configuré sur: {webhook_full_url}")
            else:
                await telegram_application.bot.delete_webhook(drop_pending_updates=True)
                logger.info("🔄 Mode polling activé (développement local)")
                asyncio.create_task(telegram_application.updater.start_polling(
                    poll_interval=1.0,
                    timeout=10,
                    drop_pending_updates=True
                ))

        except Exception as e:
            logger.error(f"❌ Erreur critique au démarrage du bot: {e}")

    yield # Le serveur tourne ici

    # Arrêt propre
    logger.info("🛑 Arrêt du Bot Telegram...")
    if telegram_application:
        try:
            await telegram_application.stop()
            await telegram_application.shutdown()
        except Exception as e:
            logger.error(f"Erreur lors de l'arrêt du bot: {e}")


app = FastAPI(lifespan=lifespan)

# Configuration CORS pour Telegram Mini App
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://web.telegram.org",
        "https://oauth.telegram.org"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Montage des fichiers statiques pour la Mini App (JS/CSS)
# Assurez-vous que le dossier existe : app/integrations/telegram/static
app.mount("/static", StaticFiles(directory="app/integrations/telegram/static"), name="static")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 2. ROUTINES DE BASE & WEBHOOK TELEGRAM
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/health")
async def health_check():
    checks = {
        "status": "healthy",
        "postgres": False,
        "bot_ready": telegram_application is not None
    }
    try:
        conn = get_postgresql_connection()
        put_connection(conn)
        checks["postgres"] = True
    except Exception:
        checks["postgres"] = False

    if not checks["postgres"]:
        return checks, 503
    return checks

@app.get("/")
async def root():
    return {"service": "Uzeur Marketplace Server", "status": "running"}

@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    """Réception des messages Telegram (si mode Webhook actif)"""
    global telegram_application
    if telegram_application is None:
        # Évite le crash 500, renvoie 200 pour que Telegram arrête de réessayer
        logger.error("❌ Bot non initialisé")
        return {"ok": True}

    try:
        data = await request.json()
        update = Update.de_json(data, telegram_application.bot)
        await telegram_application.process_update(update)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Error processing update: {e}")
        return {"ok": False}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 3. AUTHENTIFICATION MINI APP (CORRIGÉE)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def verify_telegram_webapp_data(init_data: str) -> bool:
    """
    Vérifie l'intégrité des données reçues de la WebApp Telegram.
    Utilise parse_qsl pour gérer correctement le décodage URL.
    """
    # SKIP AUTH EN DEV LOCAL
    webapp_url = os.getenv('WEBAPP_URL', '')
    if 'localhost' in webapp_url or '127.0.0.1' in webapp_url:
        logger.warning("⚠️ DEV MODE: Skipping WebApp auth")
        return True

    if not init_data:
        return False

    try:
        # 1. Parsing correct des données URL-encodées
        parsed_data = dict(parse_qsl(init_data, keep_blank_values=True))

        # 2. Extraction du hash reçu
        received_hash = parsed_data.pop('hash', None)
        if not received_hash:
            return False

        # 3. Vérification expiration (24h max)
        auth_date = int(parsed_data.get('auth_date', 0))
        if (datetime.now().timestamp() - auth_date) > 86400:
             logger.warning("⚠️ Telegram WebApp data expired")
             return False

        # 4. Reconstruction de la chaîne de vérification
        # Format: key=value triés par clé, séparés par \n
        data_check_string = '\n'.join(
            f"{k}={v}" for k, v in sorted(parsed_data.items())
        )

        # 5. Calcul HMAC-SHA256
        secret_key = hmac.new(
            "WebAppData".encode(),
            core_settings.TELEGRAM_BOT_TOKEN.encode(),
            hashlib.sha256
        ).digest()

        calculated_hash = hmac.new(
            secret_key,
            data_check_string.encode(),
            hashlib.sha256
        ).hexdigest()

        # 6. Comparaison
        is_valid = calculated_hash == received_hash

        if is_valid:
            logger.info(f"✅ WebApp Auth Success User: {parsed_data.get('user')}")
        else:
            logger.warning(f"❌ WebApp Auth Failed. Calc: {calculated_hash} != Recv: {received_hash}")

        return is_valid

    except Exception as e:
        logger.error(f"❌ Auth Exception: {e}")
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 4. API MINI APP (UPLOAD FLOW)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class GenerateUploadURLRequest(BaseModel):
    file_name: str
    file_type: str
    user_id: int
    telegram_init_data: str

class GetB2UploadURLRequest(BaseModel):
    object_key: str
    content_type: str
    user_id: int
    telegram_init_data: str

class UploadCompleteRequest(BaseModel):
    object_key: str
    file_name: str
    file_size: int
    user_id: int
    telegram_init_data: str
    preview_url: Optional[str] = None  # URL aperçu PDF généré côté client

class ClientErrorRequest(BaseModel):
    error_type: str
    details: dict
    user_id: int

@app.post("/api/generate-upload-url")
async def generate_upload_url(request: GenerateUploadURLRequest):
    """Étape 1: Le frontend demande une URL d'upload B2 Native API (CORS-compatible)"""
    if not verify_telegram_webapp_data(request.telegram_init_data):
        raise HTTPException(status_code=401, detail="Unauthorized - Invalid Init Data")

    try:
        from app.core.utils import generate_product_id
        from app.core.file_validation import validate_file_extension

        # 🔒 SÉCURITÉ: Valider l'extension AVANT tout traitement
        is_valid, error_msg = validate_file_extension(request.file_name)
        if not is_valid:
            logger.warning(f"🚫 MINIAPP: File rejected for user {request.user_id}: {request.file_name} - {error_msg}")
            raise HTTPException(status_code=400, detail=error_msg)

        # Générer product_id AVANT l'upload (critique pour chemins cohérents)
        product_id = generate_product_id()
        logger.info(f"🆔 Generated product_id BEFORE upload: {product_id} for user {request.user_id}")

        # Stocker product_id dans user_state pour upload-complete
        global telegram_application
        if telegram_application:
            bot_instance = telegram_application.bot_data.get('bot_instance')
            if bot_instance:
                user_state = bot_instance.get_user_state(request.user_id)
                product_data = user_state.get('product_data', {})
                product_data['product_id'] = product_id
                bot_instance.update_user_state(request.user_id, product_data=product_data)
                logger.info(f"✅ product_id stored in user_state: {product_id}")

        # Nettoyage du filename (garder l'extension)
        ext = request.file_name.split('.')[-1] if '.' in request.file_name else 'bin'
        clean_filename = f"main_file.{ext}"

        # ✅ NOUVELLE STRUCTURE: products/seller_id/product_id/main_file.ext
        object_key = f"products/{request.user_id}/{product_id}/{clean_filename}"

        # Appel service B2 Native API
        b2 = B2StorageService()
        upload_data = b2.get_native_upload_url(
            object_key,
            content_type=request.file_type or 'application/octet-stream'
        )

        if not upload_data:
            logger.error(
                f"❌ B2 Native upload URL generation failed\n"
                f"   User: {request.user_id}\n"
                f"   Product ID: {product_id}\n"
                f"   File: {request.file_name}\n"
                f"   Type: {request.file_type}\n"
                f"   Object key: {object_key}"
            )
            raise HTTPException(status_code=500, detail="B2 Upload URL generation failed")

        logger.info(f"✅ Generated B2 Native upload URL: {object_key}")

        return {
            "upload_url": upload_data['upload_url'],
            "authorization_token": upload_data['authorization_token'],
            "object_key": upload_data['object_key'],
            "content_type": upload_data['content_type'],
            "product_id": product_id  # ✅ Retourné au frontend pour preview
        }
    except Exception as e:
        logger.error(f"❌ Error generating URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/get-b2-upload-url")
async def get_b2_upload_url(request: GetB2UploadURLRequest):
    """Obtenir URL B2 pour un chemin spécifique (preview, etc.)"""
    if not verify_telegram_webapp_data(request.telegram_init_data):
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        # Appel service B2 pour ce chemin spécifique
        b2 = B2StorageService()
        upload_data = b2.get_native_upload_url(
            request.object_key,
            content_type=request.content_type
        )

        if not upload_data:
            logger.error(f"❌ B2 upload URL failed for path: {request.object_key}")
            raise HTTPException(status_code=500, detail="B2 Upload URL failed")

        logger.info(f"✅ B2 upload URL generated for path: {request.object_key}")

        return {
            "upload_url": upload_data['upload_url'],
            "authorization_token": upload_data['authorization_token'],
            "object_key": upload_data['object_key'],
            "content_type": upload_data['content_type']
        }
    except Exception as e:
        logger.error(f"❌ Error getting B2 URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/log-client-error")
async def log_client_error(request: ClientErrorRequest):
    """Endpoint pour recevoir et logger les erreurs JavaScript côté client"""
    logger.error(
        f"❌ CLIENT ERROR - User {request.user_id} - Type: {request.error_type}\n"
        f"   Details: {json.dumps(request.details, indent=2)}"
    )
    return {"status": "logged"}

@app.post("/api/upload-complete")
async def upload_complete(request: UploadCompleteRequest):
    """Étape 2: Le frontend confirme que l'upload est fini - Création du produit"""
    logger.info(f"🔵 START upload-complete - User: {request.user_id}, File: {request.file_name}, Size: {request.file_size}")

    if not verify_telegram_webapp_data(request.telegram_init_data):
        logger.error(f"❌ Auth failed for user {request.user_id}")
        raise HTTPException(status_code=401, detail="Unauthorized")

    logger.info(f"✅ Auth OK for user {request.user_id}")

    try:
        from telegram import InlineKeyboardButton, InlineKeyboardMarkup

        # Vérification B2
        logger.info(f"🔍 Checking B2 file existence: {request.object_key}")
        b2 = B2StorageService()
        if not b2.file_exists(request.object_key):
            logger.error(f"❌ File not found on B2: {request.object_key}")
            raise HTTPException(status_code=404, detail="File not found on B2 after upload")

        logger.info(f"✅ Storage file exists: {request.object_key}")

        # URL du fichier (R2 ou B2 selon configuration)
        if b2.storage_type == 'r2':
            custom_domain = os.getenv('R2_CUSTOM_DOMAIN', 'https://media.uzeur.com')
            file_url = f"{custom_domain}/{request.object_key}"
            logger.info(f"📦 R2 URL constructed: {file_url}")
        else:
            # Utiliser b2.bucket_name depuis l'instance B2StorageService
            file_url = f"{core_settings.B2_ENDPOINT}/{b2.bucket_name}/{request.object_key}"
            logger.info(f"📦 B2 URL constructed: {file_url}")

        global telegram_application
        logger.info(f"🤖 telegram_application exists: {telegram_application is not None}")

        if telegram_application:
            bot_instance = telegram_application.bot_data.get('bot_instance')
            logger.info(f"🤖 bot_instance exists: {bot_instance is not None}")

            if bot_instance:
                # Récupérer product_data qui contient déjà titre, description, prix, etc.
                logger.info(f"📊 Getting user state for user {request.user_id}")
                user_state = bot_instance.get_user_state(request.user_id)
                product_data = user_state.get('product_data', {})
                lang = user_state.get('lang', 'fr')

                logger.info(f"📦 Retrieved product_data: {product_data}")
                logger.info(f"🌐 Language: {lang}")

                # Validation prix minimum (0 ou >= 9.99)
                price_usd = product_data.get('price_usd', 0.0)
                if price_usd > 0 and price_usd < 9.99:
                    logger.error(f"[VALIDATION] Invalid price {price_usd} for product {product_data.get('title', 'N/A')}")
                    raise HTTPException(status_code=400, detail="Prix minimum: 9.99$ pour produits payants")

                # ✅ Utiliser product_id PRÉ-GÉNÉRÉ (stocké dans generate-upload-url)
                product_id = product_data.get('product_id')

                if not product_id:
                    logger.error(f"❌ product_id not found in product_data! This should never happen.")
                    raise HTTPException(status_code=500, detail="Product ID not found in state")

                logger.info(f"🆔 Using pre-generated product_id: {product_id}")

                # Ajouter les infos du fichier uploadé
                product_data['file_name'] = request.file_name
                product_data['file_size'] = request.file_size
                product_data['main_file_url'] = file_url
                product_data['seller_id'] = request.user_id

                logger.info(f"📝 Updated product_data with file info: file_name={request.file_name}, file_size={request.file_size}")

                # Ajouter preview_url si fourni (PDF uniquement)
                if request.preview_url:
                    product_data['preview_url'] = request.preview_url
                    logger.info(f"📸 Preview URL received: {request.preview_url}")

                # ✅ Finaliser la création du produit avec product_id existant
                logger.info(f"🔨 Calling create_product with pre-generated ID: {product_id}")
                returned_product_id = bot_instance.create_product(product_data)
                logger.info(f"🎯 create_product returned: {returned_product_id}")

                # Vérifier que l'ID retourné correspond bien
                if returned_product_id != product_id:
                    logger.warning(f"⚠️ Mismatch: Expected {product_id}, got {returned_product_id}")
                    product_id = returned_product_id  # Utiliser celui retourné

                if product_id:
                    logger.info(f"✅ Product created successfully: {product_id}")

                    # Réinitialiser l'état utilisateur
                    logger.info(f"🔄 Resetting user state for {request.user_id}")
                    bot_instance.reset_user_state_preserve_login(request.user_id)

                    # Envoyer emails de notification
                    try:
                        from app.core.email_service import EmailService
                        from app.domain.repositories.user_repo import UserRepository
                        from app.domain.repositories.product_repo import ProductRepository

                        email_service = EmailService()
                        user_repo = UserRepository()
                        product_repo = ProductRepository()

                        user_data = user_repo.get_user(request.user_id)

                        if user_data and user_data.get('email'):
                            await email_service.send_product_added_email(
                                to_email=user_data['email'],
                                seller_name=user_data.get('seller_name', 'Vendeur'),
                                product_title=product_data['title'],
                                product_price=f"{product_data['price_usd']:.2f}",
                                product_id=product_id
                            )

                            # Email premier produit si applicable
                            total_products = product_repo.count_products_by_seller(request.user_id)
                            if total_products == 1:
                                await email_service.send_first_product_published_notification(
                                    to_email=user_data['email'],
                                    seller_name=user_data.get('seller_name', 'Vendeur'),
                                    product_title=product_data['title'],
                                    product_price=product_data['price_usd']
                                )
                    except Exception as e:
                        logger.error(f"Erreur envoi emails produit: {e}")

                    # Message de succès (fonction unifiée)
                    from app.integrations.telegram.utils.message_utils import create_product_success_message
                    success_msg, keyboard = create_product_success_message(
                        product_id=product_id,
                        title=product_data['title'],
                        price=product_data['price_usd'],
                        lang=lang
                    )
                    logger.info(f"💬 Preparing success message: {success_msg}")

                    logger.info(f"📤 Sending Telegram message to {request.user_id}")
                    await telegram_application.bot.send_message(
                        chat_id=request.user_id,
                        text=success_msg,
                        reply_markup=keyboard,
                        parse_mode='Markdown'
                    )
                    logger.info(f"✅ Telegram message sent successfully to {request.user_id}")
                else:
                    logger.error(f"❌ create_product returned None for user {request.user_id}")
                    # Erreur création produit
                    await telegram_application.bot.send_message(
                        chat_id=request.user_id,
                        text="❌ Erreur lors de la création du produit"
                    )
            else:
                logger.error(f"❌ bot_instance is None!")
        else:
            logger.error(f"❌ telegram_application is None!")

        logger.info(f"🎉 END upload-complete - Success!")
        return {"status": "success", "product_id": product_id if 'product_id' in locals() else None}

    except Exception as e:
        logger.error(f"❌ Error completing upload: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5. DOWNLOAD API (MINI APP - RAILWAY-PROOF)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class VerifyPurchaseRequest(BaseModel):
    product_id: str
    user_id: int
    telegram_init_data: str

class GenerateDownloadURLRequest(BaseModel):
    product_id: str
    order_id: str
    user_id: int
    telegram_init_data: str

@app.post("/api/verify-purchase")
async def verify_purchase(request: VerifyPurchaseRequest):
    """
    Vérifie qu'un utilisateur a acheté un produit
    Utilisé par MiniApp download pour validation avant téléchargement
    """
    logger.info(f"🔍 [VERIFY-API] Request received: user_id={request.user_id}, product_id={request.product_id}")

    # 1. Authentification Telegram
    auth_valid = verify_telegram_webapp_data(request.telegram_init_data)
    logger.info(f"🔐 [VERIFY-API] Auth validation result: {auth_valid}")

    if not auth_valid:
        logger.error(f"❌ [VERIFY-API] Auth failed for user {request.user_id}")
        raise HTTPException(status_code=401, detail="Unauthorized - Invalid Init Data")

    try:
        # 2. Vérifier l'achat dans la DB
        from app.domain.repositories.product_repo import ProductRepository

        logger.info(f"💾 [VERIFY-API] Querying DB for user {request.user_id}, product {request.product_id}")
        conn = get_postgresql_connection()
        try:
            cursor = conn.cursor()

            # Query similaire à library_handlers.py:212-218
            cursor.execute('''
                SELECT
                    p.product_id,
                    p.title,
                    p.file_size_mb,
                    p.main_file_url,
                    o.order_id,
                    o.download_count,
                    o.last_download_at
                FROM orders o
                JOIN products p ON o.product_id = p.product_id
                WHERE o.buyer_user_id = %s
                  AND o.product_id = %s
                  AND o.payment_status = 'completed'
                LIMIT 1
            ''', (request.user_id, request.product_id))

            result = cursor.fetchone()

            if not result:
                logger.warning(f"⚠️ [VERIFY-API] No completed purchase found for user {request.user_id}, product {request.product_id}")
                raise HTTPException(
                    status_code=404,
                    detail="Product not purchased or payment not completed"
                )

            product_id, title, file_size_mb, main_file_url, order_id, download_count, last_download_at = result

            logger.info(f"✅ [VERIFY-API] Purchase verified: order_id={order_id}, title={title}, has_file={bool(main_file_url)}")

            # 3. Retourner les infos pour le MiniApp
            response_data = {
                "valid": True,
                "product_id": product_id,
                "product_title": title,
                "file_size_mb": file_size_mb,
                "order_id": order_id,
                "download_count": download_count or 0,
                "last_download_at": last_download_at.isoformat() if last_download_at else None,
                "has_file": bool(main_file_url)
            }
            logger.info(f"📤 [VERIFY-API] Returning response: {response_data}")
            return response_data

        finally:
            put_connection(conn)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ [VERIFY-API] Exception: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/api/generate-download-token")
async def generate_download_token(request: GenerateDownloadURLRequest):
    """
    Generate a one-time download token (uses DownloadRepository)
    Frontend will redirect to GET /download/{token}
    """
    logger.info(f"[TOKEN] Request: user_id={request.user_id}, order_id={request.order_id}, product_id={request.product_id}")

    # Verify auth
    if not verify_telegram_webapp_data(request.telegram_init_data):
        logger.error(f"[TOKEN] Auth failed for user {request.user_id}")
        raise HTTPException(status_code=401, detail="Unauthorized")

    # Rate limiting (10 tokens per hour)
    is_allowed, error_msg = DownloadRepository.check_and_update_rate_limit(request.user_id, max_tokens=10, window_seconds=3600)
    if not is_allowed:
        logger.error(f"[TOKEN] {error_msg}")
        raise HTTPException(status_code=429, detail="Too many download requests. Please try again later.")

    # Verify order ownership
    logger.info(f"[TOKEN] Verifying order ownership...")
    order_info = DownloadRepository.verify_order_ownership(request.order_id, request.user_id)

    if not order_info:
        logger.error(f"[TOKEN] Order not found: order={request.order_id}, user={request.user_id}")
        raise HTTPException(status_code=404, detail="Order not found")

    # Create token
    token = DownloadRepository.create_download_token(
        user_id=request.user_id,
        order_id=request.order_id,
        product_id=request.product_id,
        expires_minutes=5
    )

    logger.info(f"[TOKEN] Token generated: {token}")
    return {'download_token': token}


@app.get("/download/{token}")
async def download_file_with_token(token: str):
    """
    Download file using one-time token - DIRECT B2 redirect (no Railway bandwidth)
    Generates presigned B2 URL and redirects browser directly to B2
    """
    logger.info(f"[DOWNLOAD-GET] Request with token: {token}")

    # Validate and consume token (one-time use)
    token_data = DownloadRepository.get_and_validate_token(token)

    if not token_data:
        logger.error(f"[DOWNLOAD-GET] Invalid, expired, or already used token: {token}")
        raise HTTPException(status_code=404, detail="Invalid or expired token")

    user_id, order_id, product_id = token_data
    logger.info(f"[DOWNLOAD-GET] Token valid, user {user_id}, order {order_id}")

    # Get file info
    order_info = DownloadRepository.verify_order_ownership(order_id, user_id)

    if not order_info:
        raise HTTPException(status_code=404, detail="Order not found")

    main_file_url, title, file_size_mb = order_info

    if not main_file_url:
        raise HTTPException(status_code=404, detail="File not available")

    # Extract object_key from storage URL (R2 or B2)
    if "r2.cloudflarestorage.com" in main_file_url:
        r2_bucket = os.getenv('R2_BUCKET_NAME', 'uzeur')
        if f"/{r2_bucket}/" in main_file_url:
            object_key = main_file_url.split(f"/{r2_bucket}/")[1]
        else:
            object_key = main_file_url.split(f"{r2_bucket}/")[-1]
    elif "backblazeb2.com" in main_file_url:
        b2_bucket = os.getenv('B2_BUCKET_NAME')
        if f"/{b2_bucket}/" in main_file_url:
            object_key = main_file_url.split(f"/{b2_bucket}/")[1]
        else:
            object_key = main_file_url.split('.com/')[-1]
    else:
        object_key = main_file_url.split('.com/')[-1]

    object_key = object_key.split('?')[0]  # Remove query params

    # Increment download counter
    DownloadRepository.increment_download_count(order_id)

    # Generate presigned URL (direct download, no Railway proxy)
    logger.info(f"[DOWNLOAD-GET] Generating presigned URL for: {object_key}")
    b2_service = B2StorageService()

    # 2 hour expiration for large files (10GB with slow connection)
    presigned_url = b2_service.get_download_url(object_key, expires_in=7200)

    if not presigned_url:
        logger.error(f"[DOWNLOAD-GET] Failed to generate presigned URL")
        raise HTTPException(status_code=500, detail="Failed to generate download URL")

    logger.info(f"[DOWNLOAD-GET] Redirecting to B2 direct download (no Railway bandwidth)")

    # Redirect directly to B2 (browser downloads from B2, not Railway)
    return RedirectResponse(url=presigned_url, status_code=302)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 5.5 IMPORT API (GUMROAD IMPORT MINI-APP)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@app.get("/api/categories")
async def get_categories():
    """Get all categories from database"""
    try:
        import psycopg2.extras
        from app.core.db_pool import get_connection, put_connection

        conn = get_connection()
        try:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute('SELECT name FROM categories ORDER BY name')
            categories = cursor.fetchall()

            logger.info(f"[CATEGORIES] Retrieved {len(categories)} categories from DB")
            return {"categories": [cat['name'] for cat in categories]}
        finally:
            put_connection(conn)

    except Exception as e:
        logger.error(f"[CATEGORIES] Error fetching categories: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch categories")


@app.get("/api/import-products")
async def get_import_products(user_id: int, request: Request):
    """Récupérer les produits scrapés pour l'import depuis user_state"""
    logger.info(f"[IMPORT-API] Fetching products for user {user_id}")

    # Verify Telegram WebApp auth
    init_data = request.headers.get('X-Telegram-Init-Data', '')
    if not verify_telegram_webapp_data(init_data):
        logger.error(f"[IMPORT-API] Auth failed for user {user_id}")
        raise HTTPException(status_code=401, detail="Unauthorized")

    global telegram_application
    if not telegram_application:
        raise HTTPException(status_code=500, detail="Bot not initialized")

    bot_instance = telegram_application.bot_data.get('bot_instance')
    if not bot_instance:
        raise HTTPException(status_code=500, detail="Bot instance not found")

    # Get user state
    user_state = bot_instance.get_user_state(user_id)
    products = user_state.get('import_products', [])

    if not products:
        logger.warning(f"[IMPORT-API] No products found for user {user_id}")
        return {"products": []}

    logger.info(f"[IMPORT-API] Returning {len(products)} products for user {user_id}")
    return {"products": products}


class ImportCompleteRequest(BaseModel):
    object_key: str
    file_name: str
    file_size: int
    user_id: int
    telegram_init_data: str
    product_metadata: dict  # {title, description, price, category, imported_from, imported_url, cover_image_url}
    preview_url: Optional[str] = None  # URL apercu PDF genere cote client


@app.post("/api/import-complete")
async def import_complete(request: ImportCompleteRequest):
    """
    Finaliser l'import d'un produit Gumroad
    Similaire à upload-complete mais avec métadonnées pré-remplies
    """
    logger.info(f"[IMPORT-COMPLETE] User: {request.user_id}, File: {request.file_name}")

    if not verify_telegram_webapp_data(request.telegram_init_data):
        logger.error(f"[IMPORT-COMPLETE] Auth failed for user {request.user_id}")
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        # Verify file exists on B2
        logger.info(f"[IMPORT-COMPLETE] Checking B2 file: {request.object_key}")
        b2 = B2StorageService()
        if not b2.file_exists(request.object_key):
            logger.error(f"[IMPORT-COMPLETE] File not found: {request.object_key}")
            raise HTTPException(status_code=404, detail="File not found on B2")

        # Construct file URL
        if b2.storage_type == 'r2':
            custom_domain = os.getenv('R2_CUSTOM_DOMAIN', 'https://media.uzeur.com')
            file_url = f"{custom_domain}/{request.object_key}"
        else:
            # Utiliser b2.bucket_name depuis l'instance B2StorageService
            file_url = f"{core_settings.B2_ENDPOINT}/{b2.bucket_name}/{request.object_key}"

        logger.info(f"[IMPORT-COMPLETE] File URL: {file_url}")

        global telegram_application
        if not telegram_application:
            raise HTTPException(status_code=500, detail="Bot not initialized")

        bot_instance = telegram_application.bot_data.get('bot_instance')
        if not bot_instance:
            raise HTTPException(status_code=500, detail="Bot instance not found")

        # Get user state for source_profile
        user_state = bot_instance.get_user_state(request.user_id)
        source_profile = user_state.get('import_source_url', '')

        # Extraire product_id depuis object_key (genere par generate-upload-url)
        # Format: products/{user_id}/{product_id}/main_file.ext
        product_id = request.object_key.split('/')[2]
        logger.info(f"[IMPORT-COMPLETE] product_id extrait de object_key: {product_id}")

        # Prepare product data from metadata
        metadata = request.product_metadata

        # Validation prix minimum (0 ou >= 9.99)
        price = metadata.get('price', 0.0)
        if price > 0 and price < 9.99:
            logger.error(f"[IMPORT-COMPLETE] Invalid price {price} for product {metadata.get('title', 'N/A')}")
            raise HTTPException(status_code=400, detail="Prix minimum: 9.99$ pour produits payants")

        # Validation catégorie obligatoire et valide
        category = metadata.get('category', None)
        if not category:
            logger.error(f"[IMPORT-COMPLETE] Category missing for product {metadata.get('title', 'N/A')}")
            raise HTTPException(status_code=400, detail="Categorie requise")

        # Vérifier que catégorie existe en DB
        from app.core.db_pool import get_connection, put_connection
        conn = get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT name FROM categories WHERE name = %s', (category,))
            if not cursor.fetchone():
                logger.error(f"[IMPORT-COMPLETE] Invalid category {category} for product {metadata.get('title', 'N/A')}")
                raise HTTPException(status_code=400, detail=f"Categorie invalide: {category}")
        finally:
            put_connection(conn)

        # Cover image : uploadee par le frontend (mini-app) directement sur R2
        cover_image_url = None
        thumbnail_url = None
        cover_object_key = metadata.get('cover_object_key')

        if cover_object_key:
            # Cas principal : frontend a uploade l'image sur R2, on reconstruit l'URL
            if b2.storage_type == 'r2':
                custom_domain = os.getenv('R2_CUSTOM_DOMAIN', 'https://media.uzeur.com')
                cover_image_url = f"{custom_domain}/{cover_object_key}"
            else:
                cover_image_url = f"{core_settings.B2_ENDPOINT}/{b2.bucket_name}/{cover_object_key}"
            thumbnail_url = cover_image_url.replace('/cover.jpg', '/thumb.jpg')
            logger.info(f"[IMPORT-COMPLETE] Cover from frontend upload: {cover_image_url}")
        else:
            # Fallback : essayer de telecharger depuis Gumroad server-side
            gumroad_image_url = metadata.get('cover_image_url') or metadata.get('image_url')
            gumroad_product_url = metadata.get('imported_url') or metadata.get('gumroad_url')
            logger.warning(f"[IMPORT-COMPLETE] No cover_object_key, fallback download. gumroad_image_url={gumroad_image_url}")

            if gumroad_image_url and gumroad_image_url.startswith('http'):
                try:
                    from app.services.gumroad_scraper import download_cover_image
                    cover_image_url = await download_cover_image(
                        gumroad_image_url,
                        product_id,
                        seller_id=request.user_id,
                        referer_url=gumroad_product_url
                    )
                    if cover_image_url:
                        thumbnail_url = cover_image_url.replace('/cover.jpg', '/thumb.jpg')
                        logger.info(f"[IMPORT-COMPLETE] Cover downloaded server-side: {cover_image_url}")
                    else:
                        cover_image_url = gumroad_image_url
                        thumbnail_url = gumroad_image_url
                except Exception as e:
                    logger.error(f"[IMPORT-COMPLETE] Server-side cover download failed: {e}")
                    cover_image_url = gumroad_image_url
                    thumbnail_url = gumroad_image_url

        product_data = {
            'product_id': product_id,
            'seller_id': request.user_id,
            'title': metadata.get('title', 'Sans titre'),
            'description': metadata.get('description', ''),
            'price_usd': metadata.get('price', 0.0),
            'category': category,  # Déjà validée ci-dessus
            'main_file_url': file_url,
            'file_size': request.file_size,
            'file_name': request.file_name,
            'cover_image_url': cover_image_url,
            'thumbnail_url': thumbnail_url,
            'preview_url': request.preview_url,
            'imported_from': metadata.get('imported_from', 'gumroad'),
            'imported_url': metadata.get('imported_url'),
            'source_profile': source_profile,
        }

        logger.info(f"[IMPORT-COMPLETE] Creating product: {product_data['title']} cover={cover_image_url} thumb={thumbnail_url} preview={request.preview_url}")

        # Create product
        returned_product_id = bot_instance.create_product(product_data)

        if returned_product_id:
            logger.info(f"[IMPORT-COMPLETE] ✅ Product created: {returned_product_id}")

            # Send email notifications
            try:
                from app.core.email_service import EmailService
                from app.domain.repositories.user_repo import UserRepository

                email_service = EmailService()
                user_repo = UserRepository()

                user_data = user_repo.get_user(request.user_id)

                if user_data and user_data.get('email'):
                    await email_service.send_product_added_email(
                        to_email=user_data['email'],
                        seller_name=user_data.get('seller_name', 'Vendeur'),
                        product_title=product_data['title'],
                        product_price=f"{product_data['price_usd']:.2f}",
                        product_id=returned_product_id
                    )
                    logger.info(f"[IMPORT-COMPLETE] Email produit ajout envoye a {user_data['email']}")
            except Exception as e:
                logger.error(f"[IMPORT-COMPLETE] Erreur envoi emails produit: {e}")

            # Send notification to user
            try:
                # Get user language
                lang = user_state.get('lang', 'fr')

                # Message de succès (fonction unifiée)
                from app.integrations.telegram.utils.message_utils import create_product_success_message
                success_msg, keyboard = create_product_success_message(
                    product_id=returned_product_id,
                    title=product_data['title'],
                    price=product_data['price_usd'],
                    lang=lang
                )

                await telegram_application.bot.send_message(
                    chat_id=request.user_id,
                    text=success_msg,
                    reply_markup=keyboard,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.warning(f"[IMPORT-COMPLETE] Failed to send notification: {e}")

            return {"status": "success", "product_id": returned_product_id}
        else:
            logger.error(f"[IMPORT-COMPLETE] Failed to create product")
            raise HTTPException(status_code=500, detail="Failed to create product")

    except Exception as e:
        logger.error(f"[IMPORT-COMPLETE] Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 6. IPN NOWPAYMENTS (PAIEMENTS)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def verify_ipn_signature(secret: str, payload: bytes, signature: str) -> bool:
    if not secret or not signature:
        return False
    mac = hmac.new(secret.encode(), msg=payload, digestmod=hashlib.sha512).hexdigest()
    return hmac.compare_digest(mac, signature)

async def send_formation_to_buyer(buyer_user_id: int, order_id: str, product_id: str):
    """Logique métier: Délivre le fichier acheté"""
    from app.domain.repositories.product_repo import ProductRepository

    repo = ProductRepository()
    product = repo.get_product_by_id(product_id)

    if not product or not product.get('main_file_url'):
        logger.error(f"❌ Produit introuvable ou sans fichier: {product_id}")
        return False

    # Génération lien temporaire de téléchargement (24h)
    download_link = get_b2_presigned_url(product['main_file_url'], expires_in=86400)

    msg = (
        f"🎉 **Paiement confirmé !** (Commande #{order_id})\n\n"
        f"Voici votre formation : **{product.get('title')}**\n"
        f"🔗 [Télécharger ici]({download_link})\n\n"
        f"⚠️ Lien valide 24h."
    )

    global telegram_application
    # Utilise le bot global s'il est là, sinon une instance temporaire
    bot = telegram_application.bot if telegram_application else Bot(core_settings.TELEGRAM_BOT_TOKEN)

    try:
        await bot.send_message(chat_id=buyer_user_id, text=msg, parse_mode='Markdown')
        logger.info(f"✅ Fichier envoyé à {buyer_user_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Echec envoi fichier: {e}")
        return False

@app.post("/ipn/nowpayments")
async def nowpayments_ipn(request: Request):
    """Réception des notifications de paiement NowPayments"""
    # 1. Vérification Signature
    raw_body = await request.body()
    signature = request.headers.get('x-nowpayments-sig')

    if not verify_ipn_signature(core_settings.NOWPAYMENTS_IPN_SECRET, raw_body, signature):
        logger.warning("⚠️ IPN Invalid Signature")
        raise HTTPException(status_code=401, detail="Invalid Signature")

    try:
        data = json.loads(raw_body.decode())
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    # 2. Analyse du statut
    payment_status = data.get('payment_status')
    order_id = data.get('order_id') # ID interne
    payment_id = data.get('payment_id') # ID NowPayments

    logger.info(f"💰 IPN reçu: Order {order_id} - Status {payment_status}")

    # On ne traite que les succès
    if payment_status not in ['finished', 'confirmed']:
        return {"status": "ignored", "reason": f"Status is {payment_status}"}

    # 3. Mise à jour Base de Données via Repositories
    try:
        order_repo = OrderRepository()
        payout_service = SellerPayoutService()

        # Vérifier si l'order existe
        order = order_repo.get_order_by_id(order_id)

        if not order:
            logger.error(f"❌ Order {order_id} not found in DB")
            return {"status": "error", "message": "Order not found"}

        payment_status = order.get('payment_status')
        buyer_user_id = order.get('buyer_user_id')
        product_id = order.get('product_id')

        # Vérifier si déjà traité
        if payment_status == 'completed':
            logger.info(f"ℹ️ Commande {order_id} déjà complétée")
            return {"status": "ok", "message": "Already completed"}

        # Mettre à jour le statut (incrémente automatiquement sales_count, total_sales, total_revenue)
        success = order_repo.update_payment_status(order_id, 'completed', payment_id)

        if not success:
            logger.error(f"❌ Failed to update payment status for order {order_id}")
            raise HTTPException(status_code=500, detail="Failed to update order")

        logger.info(f"✅ Order {order_id} marked as completed - sales_count incremented")

        # Créer le payout pour le vendeur
        payout_id = await payout_service.create_payout_from_order_async(order_id)

        if payout_id:
            logger.info(f"✅ Payout {payout_id} created for order {order_id}")
        else:
            logger.warning(f"⚠️ Could not create payout for order {order_id} (seller may not have wallet configured)")

        # 4. Livraison du produit
        await send_formation_to_buyer(buyer_user_id, order_id, product_id)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error processing IPN: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal Server Error")

    return {"status": "ok"}

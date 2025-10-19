from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.cards import router as cards_router
 
def get_application():
    app = FastAPI(title="Planeswalker", version="1.0.0")
 
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(cards_router, "/cards")
 
    return app



app = get_application()
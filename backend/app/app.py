from __future__ import annotations

import uvicorn
from fastapi import FastAPI

from .routers import health, prices, screener


def create_app() -> FastAPI:
    app = FastAPI(title="Quant Trading API", version="1.0.0")

    # 路由註冊
    app.include_router(health.router)
    app.include_router(prices.router)
    app.include_router(screener.router)

    return app


app = create_app()
 
# uvicorn backend.app.app:app --reload    
def main() -> None:
    uvicorn.run(
        "backend.app.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        workers=1,
    )


if __name__ == "__main__":
    main()

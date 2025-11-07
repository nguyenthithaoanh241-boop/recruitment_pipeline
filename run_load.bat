@echo off
echo --- Dang kich hoat venv ---
CALL "venv\Scripts\activate.bat"

echo.
echo --- [BUOC 1/1] === DANG NAP DU LIEU (LOADER) === ---
python pipeline\loader.py

echo.
echo --- [HOAN TAT] ---

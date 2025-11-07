@echo off
echo --- Dang kich hoat venv ---
CALL "venv\Scripts\activate.bat"

echo.
echo --- [BUOC 1/1] === DANG THIET LAP DATABASE === ---
python pipeline\db_setup.py

echo.
echo --- [HOAN TAT] ---
PAUSE
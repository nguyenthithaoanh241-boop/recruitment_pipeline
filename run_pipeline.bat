@echo off
TITLE Chay Pipeline Tuyendung
echo --- [BAT DAU PIPELINE TUYEN DUNG] ---

echo.
echo --- Dang kich hoat venv ---
CALL "venv\Scripts\activate.bat"
IF ERRORLEVEL 1 (
    echo !!! LOI: Khong tim thay "venv\Scripts\activate.bat"
    GOTO :EOF
)

echo.
echo [BUOC 1/3] === DANG CHAY TOPCV SCRAPER ===
python scrapers\TopCV.py

echo.
echo [BUOC 2/3] === DANG CHAY CAREERLINK SCRAPER (Tat ca danh muc) ===
python scrapers\Careerlink.py

:: (Ban co the them cac scraper khac o day)
echo [BUOC 3/X] === DANG CHAY CAREERVIET SCRAPER ===
python scrapers\CareerViet.py

echo.
echo [BUOC 3/3] === DANG NAP DU LIEU VAO DATABASE (LOADER) ===
python pipeline\loader.py

echo.
echo --- [HOAN TAT TOAN BO PIPELINE] ---
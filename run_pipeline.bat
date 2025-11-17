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
echo === DANG CHAY CAREERLINK SCRAPER (Tat ca danh muc) ===
python scrapers\CareerlinkM.py 


echo === DANG CHAY CAREERVIET SCRAPER ===
python scrapers\CareerViet.py


echo.
echo === DANG CHAY TOPCV SCRAPER ===
python scrapers\TopCV.py

echo  === DANG CHAY CAREERVIET SCRAPER ===
python scrapers\CareerVietC.py
ECHO Tam nghi 5 phut (300 giay) truoc khi chay scraper tiep theo...


echo.
echo === DANG CHAY CAREERLINK SCRAPER (Tat ca danh muc) ===
python scrapers\Careerlink.py 

echo.
echo  === DANG NAP DU LIEU VAO DATABASE (LOADER) ===
python push_load.py

echo.
echo --- [HOAN TAT] ---
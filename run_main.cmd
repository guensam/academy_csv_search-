@echo off
chcp 65001 >nul
cd /d "%~dp0"

REM 가상환경이 있으면 우선 사용
if exist "%~dp0.venv\Scripts\python.exe" (
  "%~dp0.venv\Scripts\python.exe" main.py
  goto :after_run
)

REM Windows: py 런처 → python 순
where py >nul 2>&1
if %errorlevel% equ 0 (
  py -3 main.py
  goto :after_run
)
where python >nul 2>&1
if %errorlevel% equ 0 (
  python main.py
  goto :after_run
)

echo [오류] Python을 찾을 수 없습니다. Python 3.11을 설치하거나 PATH에 등록하세요.
echo        또는 이 폴더에서: py -3 -m venv .venv ^& .venv\Scripts\pip install -r requirements.txt
pause
exit /b 1

:after_run
if errorlevel 1 (
  echo.
  echo [안내] 모듈 오류 시: pip install -r requirements.txt
)
echo.
pause

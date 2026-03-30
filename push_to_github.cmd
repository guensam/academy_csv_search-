@echo off
chcp 65001 >nul
cd /d "%~dp0"

REM GitHub: https://github.com/guensam/academy_csv_search-
set "GITHUB_URL=https://github.com/guensam/academy_csv_search-.git"

git remote remove origin 2>nul
git remote add origin "%GITHUB_URL%"
echo origin = %GITHUB_URL%
echo.
git push -u origin main
if errorlevel 1 (
  echo.
  echo 푸시 실패 시: GitHub 로그인(브라우저/PAT)을 확인하세요.
  echo 첫 푸시 전에 변경사항을 커밋하려면:
  echo   git add -A
  echo   git commit -m "메시지"
  echo   git push -u origin main
)
echo.
pause

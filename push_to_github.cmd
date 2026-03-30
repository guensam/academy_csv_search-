@echo off
chcp 65001 >nul
cd /d "%~dp0"

REM GitHub: https://github.com/guensam/academy_csv_search-
set "GITHUB_URL=https://github.com/guensam/academy_csv_search-.git"

git remote get-url origin >nul 2>&1
if errorlevel 1 (
  git remote add origin "%GITHUB_URL%"
) else (
  git remote set-url origin "%GITHUB_URL%"
)
echo origin = %GITHUB_URL%
echo.

echo [1/3] 변경 파일 스테이징 (git add -A^) ...
git add -A
git diff --cached --quiet
if errorlevel 1 (
  echo [2/3] 커밋 생성 ...
  git commit -m "Update academy_csv_search (%date% %time%)"
  if errorlevel 1 (
    echo.
    echo 커밋에 실패했습니다. git config user.name / user.email 을 설정했는지 확인하세요.
    pause
    exit /b 1
  )
  echo        커밋 완료.
) else (
  echo [2/3] 새로 커밋할 변경이 없습니다. ^(이미 커밋된 커밋만 push^)
)

echo [3/3] GitHub로 푸시 ...
git push -u origin main
if errorlevel 1 (
  echo.
  echo 푸시 실패: 로그인^(브라우저 / Personal Access Token^) 또는 원격 브랜치 충돌을 확인하세요.
  echo 다른 PC에서 받은 적이 있으면: git pull --rebase origin main 후 다시 이 스크립트를 실행하세요.
)
echo.
pause

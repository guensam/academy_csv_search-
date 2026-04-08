# -*- coding: utf-8 -*-
"""
학원 CSV 지역·음악/피아노 검색 도구 - GUI 버전
"""

import sys
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox

# 기존 기능 임포트
import main as core_main


class RedirectText(object):
    """표준 출력을 Tkinter Text 위젯으로 리다이렉트하는 클래스"""
    def __init__(self, text_widget):
        self.text_widget = text_widget

    def write(self, string):
        self.text_widget.configure(state='normal')
        self.text_widget.insert(tk.END, string)
        self.text_widget.see(tk.END)
        self.text_widget.configure(state='disabled')

    def flush(self):
        pass


class AcademySearchApp(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("학원 CSV 검색 도구")
        self.geometry("700x500")
        self.configure(padx=10, pady=10)

        # 폰트 설정
        default_font = ("맑은 고딕", 10)
        self.option_add("*Font", default_font)

        self._create_widgets()
        self._redirect_output()

    def _create_widgets(self):
        # 상단 입력 프레임
        top_frame = ttk.Frame(self)
        top_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Label(top_frame, text="검색할 지역명:").pack(side=tk.LEFT, padx=(0, 5))
        
        self.region_var = tk.StringVar()
        self.region_entry = ttk.Entry(top_frame, textvariable=self.region_var, width=30)
        self.region_entry.pack(side=tk.LEFT, padx=(0, 10))
        self.region_entry.bind("<Return>", lambda e: self.start_search())
        
        # 힌트 라벨
        ttk.Label(top_frame, text="(예: 안산시, 강남구 / 쉼표로 다중 지역 가능)", foreground="gray").pack(side=tk.LEFT)

        self.search_btn = ttk.Button(top_frame, text="검색 및 엑셀 저장", command=self.start_search)
        self.search_btn.pack(side=tk.RIGHT)

        # 중앙 로그 출력 프레임
        log_frame = ttk.LabelFrame(self, text="진행 로그")
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = scrolledtext.ScrolledText(log_frame, state='disabled', wrap=tk.WORD, font=("Consolas", 10))
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # 하단 버튼 프레임
        bottom_frame = ttk.Frame(self)
        bottom_frame.pack(fill=tk.X, pady=(10, 0))

        self.close_btn = ttk.Button(bottom_frame, text="창닫기", command=self.destroy)
        self.close_btn.pack(side=tk.RIGHT, padx=(5, 0))

        self.reset_btn = ttk.Button(bottom_frame, text="초기화", command=self.reset_all)
        self.reset_btn.pack(side=tk.RIGHT)

    def _redirect_output(self):
        # stdout, stderr를 텍스트 위젯으로 연결
        redir = RedirectText(self.log_text)
        sys.stdout = redir
        sys.stderr = redir

    def clear_logs(self):
        self.log_text.configure(state='normal')
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state='disabled')

    def reset_all(self):
        self.region_var.set("")
        self.clear_logs()
        self.region_entry.focus()

    def start_search(self):
        region = self.region_var.get().strip()
        if not region:
            messagebox.showwarning("입력 오류", "지역명을 입력해주세요.")
            self.region_entry.focus()
            return

        # UI 비활성화
        self.search_btn.state(['disabled'])
        self.region_entry.state(['disabled'])
        self.reset_btn.state(['disabled'])
        self.clear_logs()

        # 별도 스레드에서 실행하여 UI 멈춤 방지
        thread = threading.Thread(target=self._run_core_main, args=(region,), daemon=True)
        thread.start()

    def _run_core_main(self, region):
        try:
            # sys.exit 호출 시 스레드만 종료되도록 처리
            try:
                core_main.main(gui_region=region)
            except SystemExit as e:
                # 에러 코드가 0이 아닌 경우 출력
                if e.code is not None and e.code != 0:
                    print(f"\n[프로세스 종료됨 (코드: {e.code})]", file=sys.stderr)
                else:
                    print(f"\n[완료됨]")
        except Exception as e:
            print(f"\n예기치 않은 오류 발생: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
        finally:
            # 작업이 끝난 후 UI를 다시 활성화 (Tkinter 메인 스레드에서 안전하게 실행)
            self.after(0, self._enable_ui)

    def _enable_ui(self):
        self.search_btn.state(['!disabled'])
        self.region_entry.state(['!disabled'])
        self.reset_btn.state(['!disabled'])


if __name__ == "__main__":
    app = AcademySearchApp()
    app.mainloop()

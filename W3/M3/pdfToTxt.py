import PyPDF2

def extract_text_from_pdf(pdf_path, output_txt_path):
    try:
        # PDF 파일 열기
        with open(pdf_path, 'rb') as pdf_file:
            reader = PyPDF2.PdfReader(pdf_file)
            
            # 모든 페이지의 텍스트 추출
            extracted_text = ""
            for page in reader.pages:
                extracted_text += page.extract_text() + "\n"

        # 추출한 텍스트를 텍스트 파일에 저장
        with open(output_txt_path, 'w', encoding='utf-8') as txt_file:
            txt_file.write(extracted_text)

        print(f"텍스트가 성공적으로 추출되었습니다: {output_txt_path}")

    except Exception as e:
        print(f"오류가 발생했습니다: {e}")

# PDF 파일 경로와 출력 파일 경로 설정
pdf_path = "signal_processing_first.pdf"  # 추출할 PDF 파일 경로
output_txt_path = "SPF.txt"  # 출력 텍스트 파일 경로

extract_text_from_pdf(pdf_path, output_txt_path)

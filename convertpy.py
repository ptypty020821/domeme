import streamlit as st
import pandas as pd
from io import BytesIO
import zipfile
from difflib import SequenceMatcher

# =================================================
# Streamlit 설정
# =================================================
st.set_page_config(
    page_title="도매매 복수배송지주소록 자동 생성기",
    layout="wide"
)

st.title("📦 도매매 복수배송지주소록 자동 변환기")
st.caption("※ 1·2·3행은 절대 변경되지 않으며, 데이터는 4행부터 입력됩니다.")

# =================================================
# 1. 출력 컬럼 (3행과 100% 동일)
# =================================================
OUTPUT_COLUMNS = [
    "번호",
    "수령자명",
    "휴대전화",
    "추가연락처(선택)",
    "배송지주소",
    "배송상세주소",
    "우편번호",
    "배송요청사항(선택)",
    "쇼핑몰명(조건부필수)",
    "전달사항(선택)",
    "개인통관부호(조건부필수)",
    "상품옵션(선택)",
    "수량",
]

# =================================================
# 2. 1·2·3행 고정 내용 (절대 변경 ❌)
# =================================================
FIXED_ROWS = [
    ["도매매 복수배송지주소록"] + [""] * (len(OUTPUT_COLUMNS) - 1),

    [' ※ 기재 시 유의사항 : \n'
     '1. 복수배송지보내기는 1회당 30개 이하로 제한됩니다\n'
     '2. 사용 시 1, 2, 3행은 삭제하면 안됩니다. 4행은 예시이므로 삭제 후 이용하세요\n'
     '3. 노란색은 필수, 연두색은 선택입력사항 입니다\n'
     '4. 도매매에서 상품을 구매하는 경우 쇼핑몰명을 반드시 입력해야 하며, '
     '해외배송상품의 경우 개인통관부호가 반드시 입력되어야 합니다'
    ] + [""] * (len(OUTPUT_COLUMNS) - 1),

    OUTPUT_COLUMNS
]

# =================================================
# 3. 자동 컬럼 매핑 사전
# =================================================
COLUMN_MAP = {
    "수령자명": ["수취인이름", "수취인", "고객명", "이름"],
    "휴대전화": ["수취인전화번호", "전화번호", "연락처", "핸드폰"],
    "추가연락처(선택)": ["추가연락처", "보조연락처", "연락처2"],
    "배송지주소": ["주소", "기본주소", "수취인주소"],
    "배송상세주소": ["상세주소", "주소상세"],
    "우편번호": ["우편번호", "zip", "zipcode"],
    "배송요청사항(선택)": ["배송메세지", "배송메시지", "요청사항"],
    "쇼핑몰명(조건부필수)": ["쇼핑몰명", "판매처", "플랫폼"],
    "전달사항(선택)": ["전달사항"],
    "개인통관부호(조건부필수)": ["개인통관부호", "통관번호", "PCCC"],
    "상품옵션(선택)": ["옵션명", "옵션", "상품옵션"],
    "수량": ["수량", "구매수", "수량합계"],
}

# =================================================
# 4. 문자열 유사도
# =================================================
def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

# =================================================
# 5. 컬럼 자동 매칭
# =================================================
def find_best_match(template_col, source_cols):
    if template_col in COLUMN_MAP:
        for alias in COLUMN_MAP[template_col]:
            for src in source_cols:
                if alias.replace(" ", "").lower() in src.replace(" ", "").lower():
                    return src

    for src in source_cols:
        if template_col.replace(" ", "").lower() in src.replace(" ", "").lower():
            return src

    best_score, best_match = 0, None
    for src in source_cols:
        score = similar(template_col.lower(), src.lower())
        if score > best_score:
            best_score, best_match = score, src

    return best_match if best_score >= 0.4 else None

# =================================================
# 6. 출력 데이터 변환 (번호는 항상 새로 생성)
# =================================================
def convert_to_output(df):
    result = pd.DataFrame()
    source_cols = list(df.columns)

    for col in OUTPUT_COLUMNS:
        if col == "번호":
            continue

        # ★ 쇼핑몰명은 고정값
        if col == "쇼핑몰명(조건부필수)":
            result[col] = ["이인컴퍼니"] * len(df)
            continue

        match = find_best_match(col, source_cols)
        result[col] = df[match] if match else ""

    result.insert(0, "번호", range(1, len(result) + 1))
    return result

# =================================================
# 7. 엑셀 저장 (1~3행 고정 / 4행부터 데이터)
# =================================================
def save_domeme_xlsx(buffer, data_df):
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        sheet_name = "Sheet1"

        # 1️⃣ 1~3행을 먼저 DataFrame으로 작성 (절대 안 깨짐)
        fixed_df = pd.DataFrame(FIXED_ROWS)
        fixed_df.to_excel(
            writer,
            sheet_name=sheet_name,
            index=False,
            header=False
        )

        # 2️⃣ 4행부터 실제 데이터만 추가
        data_df.to_excel(
            writer,
            sheet_name=sheet_name,
            startrow=3,
            index=False,
            header=False
        )

# =================================================
# 8. ZIP 생성
# =================================================
def create_zip(files):
    zip_buffer = BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for filename, df in files.items():
            buffer = BytesIO()
            save_domeme_xlsx(buffer, df)
            zipf.writestr(filename, buffer.getvalue())

    zip_buffer.seek(0)
    return zip_buffer

# =================================================
# 9. UI
# =================================================
uploaded_file = st.file_uploader(
    "📁 A 파일 업로드 (xls / xlsx)",
    type=["xls", "xlsx"]
)

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    st.subheader("📌 원본 데이터 미리보기")
    st.dataframe(df.head())

    # 상품명 컬럼 자동 탐색
    candidate_cols = ["등록상품명", "상품명", "노출상품명", "제품명"]
    product_col = next(
        (c for c in df.columns if any(k in c for k in candidate_cols)),
        None
    )

    if not product_col:
        st.error("❌ 등록상품명 관련 컬럼을 찾을 수 없습니다.")
        st.stop()

    st.success(f"✔ 등록상품명 컬럼 감지: {product_col}")

    output_df = convert_to_output(df)

    st.subheader("📌 변환 결과 미리보기")
    st.dataframe(output_df.head())

    # 상품명 기준 분리
    grouped = output_df.groupby(df[product_col])
    output_files = {}

    for name, group in grouped:
        safe_name = str(name).replace("/", "_").replace("\\", "_")
        output_files[f"{safe_name}.xlsx"] = group

    st.subheader("📌 생성될 파일 목록")
    st.write(list(output_files.keys()))

    zip_file = create_zip(output_files)

    st.download_button(
        "📥 ZIP 다운로드 (1~3행 고정)",
        data=zip_file,
        file_name="도매매_복수배송지주소록.zip",
        mime="application/zip"
    )

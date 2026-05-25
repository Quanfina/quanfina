"""Mark Minervini PDF text extraction — derin tarama icin."""
import pypdf
from pathlib import Path

# Drive'ım icindeki PDF yollari (Türkçe karakter Path objesi ile)
PDF_DIR = Path("G:/Drive'ım/QUANFINA/Gemler/01_Gem")

BOOKS = {
    'TLSMW': '01_Minervini_Trade_Like_a_Stock_Market_Wizard_2013.pdf',
    'TTLC':  '02_Minervini_Think_and_Trade_Like_a_Champion_2017.pdf',
    'MSW':   '03_Minervini_Mindset_Secrets_for_Winning_2019.pdf',
    'MM':    '04_Minervini_Momentum_Masters_2015.pdf',
}


def page_counts():
    for code, fname in BOOKS.items():
        path = PDF_DIR / fname
        try:
            reader = pypdf.PdfReader(str(path))
            print(f'{code}: {len(reader.pages)} sayfa  -  {path.name}')
        except Exception as e:
            print(f'{code} HATA: {e}')


def extract_pages(book_code, start, end, out_file=None):
    """Belirli sayfa araligini text olarak cikar."""
    path = PDF_DIR / BOOKS[book_code]
    reader = pypdf.PdfReader(str(path))
    total = len(reader.pages)
    if end > total:
        end = total
    text_chunks = []
    for i in range(start - 1, end):
        try:
            txt = reader.pages[i].extract_text()
            text_chunks.append(f'\n===== {book_code} Page {i+1} =====\n{txt}')
        except Exception as e:
            text_chunks.append(f'\n===== {book_code} Page {i+1} (HATA: {e}) =====\n')
    full = '\n'.join(text_chunks)
    if out_file:
        Path(out_file).write_text(full, encoding='utf-8')
        print(f'YAZILDI: {out_file} ({len(full)} char, {end-start+1} sayfa)')
    return full


def extract_toc(book_code):
    """TOC bulmak icin ilk 20 sayfayi text olarak ver."""
    return extract_pages(book_code, 1, 20)


if __name__ == '__main__':
    import sys
    if len(sys.argv) == 1:
        page_counts()
    elif sys.argv[1] == 'toc':
        # python script.py toc TLSMW
        code = sys.argv[2] if len(sys.argv) > 2 else 'TLSMW'
        out = sys.argv[3] if len(sys.argv) > 3 else None
        print(extract_toc(code) if not out else f'TOC -> {out}')
        if out:
            extract_toc_to_file = extract_pages(code, 1, 20, out)
    elif sys.argv[1] == 'pages':
        # python script.py pages TLSMW 50 70 out.txt
        code = sys.argv[2]
        start = int(sys.argv[3])
        end = int(sys.argv[4])
        out = sys.argv[5] if len(sys.argv) > 5 else None
        result = extract_pages(code, start, end, out)
        if not out:
            print(result[:5000])

"""Mark Minervini 21 chunk text dosyasindan akilli ozet cikarici.

Her chunk icin:
- Sayfa basliklari (CHAPTER X, SECTION X)
- Mark'in birebir aliintilarini (tirnak icinde uzun cumleler)
- Sayisal esikleri (% ile sayilar, gun/hafta, dolar)
- Anahtar kavramlar (cap harf bashlik)

Cikti: tek tek ozet dosyalari + tek mega ozet
"""
import re
from pathlib import Path

CHUNK_DIR = Path("C:/Users/Ferit/AppData/Local/Temp/mark_pdfs")
OUT_DIR = Path("C:/Users/Ferit/AppData/Local/Temp/mark_summaries")
OUT_DIR.mkdir(parents=True, exist_ok=True)


# Sirali okuma siralamasi
CHUNKS_ORDER = [
    # TLSMW
    ('TLSMW_p1_20.txt', 'TLSMW Ch 1-2 (Intro + What You Need to Know)'),
    ('TLSMW_Ch3_SEPA.txt', 'TLSMW Ch 3 (SEPA Strategy)'),
    ('TLSMW_Ch4_Value.txt', 'TLSMW Ch 4 (Value Comes at a Price)'),
    ('TLSMW_Ch5_Trend.txt', 'TLSMW Ch 5 (Trading with the Trend)'),
    ('TLSMW_Ch67_Cat_Fund.txt', 'TLSMW Ch 6-7 (Categories + Fundamentals)'),
    ('TLSMW_Ch8_Earnings.txt', 'TLSMW Ch 8 (Earnings Quality)'),
    ('TLSMW_Ch9_Leaders.txt', 'TLSMW Ch 9 (Follow the Leaders)'),
    ('TLSMW_Ch10_Patterns.txt', 'TLSMW Ch 10 part 1 (Chart Pattern)'),
    ('TLSMW_Ch10_part2_Ch11.txt', 'TLSMW Ch 10 part 2 + Ch 11'),
    ('TLSMW_Ch12_13_Risk.txt', 'TLSMW Ch 12-13 (Risk Management)'),
    # TTLC
    ('TTLC_p1_20.txt', 'TTLC Intro'),
    ('TTLC_Sec1_3_Plan_Risk.txt', 'TTLC Sec 1-3 (Plan + Risk First)'),
    ('TTLC_Sec4_5_RBA_Compound.txt', 'TTLC Sec 4-5 (RBA + Compound)'),
    ('TTLC_Sec6_9_Buy_Sell.txt', 'TTLC Sec 6-9 (Buy + Sell)'),
    ('TTLC_Sec10_11.txt', 'TTLC Sec 10-11 (8 Keys + Mindset)'),
    # MSW
    ('MSW_p1_20.txt', 'MSW Intro + TOC'),
    ('MSW_intro.txt', 'MSW Bonus Chapter'),
    ('MSW_Mid1.txt', 'MSW Mid 1 (s.30-80)'),
    ('MSW_Mid2.txt', 'MSW Mid 2 (s.81-164)'),
    # MM
    ('MM_intro.txt', 'MM Intro'),
    ('MM_Mid1.txt', 'MM Mid 1 (s.40-90)'),
    ('MM_Mid2.txt', 'MM Mid 2 (s.91-157)'),
]


def extract_chapter_headings(text):
    """CHAPTER X / SECTION X / Subsection bashliklarini cikar."""
    headings = []
    for line in text.split('\n'):
        line_clean = line.strip()
        # CHAPTER X uppercase pattern
        if re.match(r'^CHAPTER\s+\d+', line_clean):
            headings.append(line_clean[:80])
        # SECTION X uppercase pattern
        if re.match(r'^SECTION\s+\d+', line_clean):
            headings.append(line_clean[:80])
        # Mark TitleCase headings (3-7 words, dont end with .)
        if re.match(r'^[A-Z][a-z]+(\s+[A-Z][a-z]+){1,6}$', line_clean) and len(line_clean) < 60:
            headings.append(line_clean)
    # Unique + first 30
    seen = set()
    uniq = []
    for h in headings:
        if h not in seen:
            seen.add(h)
            uniq.append(h)
    return uniq[:30]


def extract_quotes(text):
    """Mark'in tirnak icindeki birebir cumlelerini cikar."""
    # Match "..." or "..." with 30-300 chars
    quotes = re.findall(r'"([^"]{30,300})"|"([^"]{30,300})"', text)
    quotes = [q[0] or q[1] for q in quotes]
    # Filter book references / generic
    quotes = [q.strip() for q in quotes if not any(skip in q.lower() for skip in
              ['copyright', 'isbn', 'publisher', 'reserved'])]
    return quotes[:15]


def extract_numbers(text):
    """Sayisal esikleri cikar: %, gun/hafta, dolar, multiplier."""
    pct_pattern = r'\b(\d{1,3}\.?\d*)\s*(?:percent|%)\b'
    week_pattern = r'\b(\d{1,3})\s*(?:weeks?|days?|months?|years?)\b'
    dollar_pattern = r'\$\s*(\d[\d,]*\.?\d*)\s*(?:million|billion|share)?'

    pcts = re.findall(pct_pattern, text, re.IGNORECASE)
    weeks = re.findall(week_pattern, text, re.IGNORECASE)
    dollars = re.findall(dollar_pattern, text, re.IGNORECASE)

    return {
        'percentages': list(set(pcts))[:20],
        'time_units': list(set(weeks))[:15],
        'dollars': list(set(dollars))[:15],
    }


def extract_quanfina_relevant(text):
    """Quanfina-relevant kavramlari arar."""
    keywords = [
        'trend template', 'vcp', 'pivot', 'cup', 'handle', 'flag', 'power play',
        'sepa', 'stop loss', 'pyramid', 'breakout', 'volume', 'rs rating', 'relative strength',
        'earnings', 'sales', 'roe', 'margin', 'institutional', 'leader', 'laggard',
        'stage 1', 'stage 2', 'stage 3', 'stage 4', '200-day', '50-day',
        'distribution', 'follow.through', 'climax', 'breakdown',
        'expectancy', 'batting average', 'win rate', 'drawdown', 'kelly',
        'catalyst', 'biotech', 'ipo', 'fundamental', 'p/e ratio',
        'mindset', 'discipline', 'plan', 'risk-first',
        'sit-out', 'sit out',
    ]
    found = {}
    text_lower = text.lower()
    for kw in keywords:
        count = text_lower.count(kw.lower())
        if count > 0:
            found[kw] = count
    # Sort by count desc
    return dict(sorted(found.items(), key=lambda x: -x[1])[:25])


def process_chunk(chunk_path, chunk_label):
    text = chunk_path.read_text(encoding='utf-8', errors='ignore')

    headings = extract_chapter_headings(text)
    quotes = extract_quotes(text)
    numbers = extract_numbers(text)
    keywords = extract_quanfina_relevant(text)

    return {
        'label': chunk_label,
        'char_count': len(text),
        'headings': headings,
        'quotes': quotes,
        'numbers': numbers,
        'keywords': keywords,
    }


def format_summary(summary):
    """Markdown format ozet."""
    lines = []
    lines.append(f"## {summary['label']}")
    lines.append(f"**Boyut:** {summary['char_count']:,} char")
    lines.append("")

    lines.append("### Heading/Section listesi")
    for h in summary['headings']:
        lines.append(f"- {h}")
    lines.append("")

    lines.append("### Mark Birebir Alintilar (en onemli 15)")
    for q in summary['quotes']:
        # Single line, escape pipes
        lines.append(f"> *\"{q}\"*")
    lines.append("")

    lines.append("### Sayisal Esikler")
    lines.append(f"- **Yuzdeler (%):** {', '.join(summary['numbers']['percentages'])}")
    lines.append(f"- **Sure birimleri:** {', '.join(summary['numbers']['time_units'])}")
    lines.append(f"- **Dolar miktarlari:** {', '.join(summary['numbers']['dollars'])}")
    lines.append("")

    lines.append("### Quanfina-Relevant Keywords (frekans)")
    for kw, count in summary['keywords'].items():
        lines.append(f"- `{kw}`: {count}x")
    lines.append("")
    lines.append("---")
    lines.append("")

    return '\n'.join(lines)


def main():
    all_summaries = []
    for fname, label in CHUNKS_ORDER:
        path = CHUNK_DIR / fname
        if not path.exists():
            print(f"SKIP (yok): {fname}")
            continue
        summary = process_chunk(path, label)
        formatted = format_summary(summary)
        all_summaries.append(formatted)

        # Bireysel cikti
        out_path = OUT_DIR / f"summary_{path.stem}.md"
        out_path.write_text(formatted, encoding='utf-8')
        print(f"YAZILDI: {out_path.name} ({len(formatted)} char)")

    # Mega ozet
    mega = "# Mark 4 Kitap Tum Bolum Ozeti\n\n" + '\n'.join(all_summaries)
    mega_path = OUT_DIR / "MEGA_summary.md"
    mega_path.write_text(mega, encoding='utf-8')
    print(f"\nMEGA OZET: {mega_path} ({len(mega):,} char)")


if __name__ == '__main__':
    main()

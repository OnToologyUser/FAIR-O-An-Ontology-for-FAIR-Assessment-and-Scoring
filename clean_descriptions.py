"""
Clean dct:description literals in a Turtle (.ttl) file.
Removes: HTML tags (keeping link text), \r, markdown syntax, excess whitespace.
"""
import re
import sys

INPUT  = "data/fair_assessment_kg.ttl"
OUTPUT = "data/fair_assessment_kg.ttl"


def clean_text(text: str) -> str:

    text = text.replace("\\r", " ")
    text = text.replace("\\n", " ")


    text = text.replace("\r", " ")

    text = re.sub(r'<a\b[^>]*>(.*?)</a>', r'\1', text, flags=re.IGNORECASE | re.DOTALL)

    text = re.sub(r'<[^>]+>', ' ', text)

    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)

    text = re.sub(r'^\s*#{1,6}\s+', '', text, flags=re.MULTILINE)

    text = re.sub(r'^\s*[*\-+]\s+', '', text, flags=re.MULTILINE)

    text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)

    text = re.sub(r'\n([,;:.])', r'\1', text)

    paragraphs = text.split('\n\n')
    joined = []
    for para in paragraphs:

        para = re.sub(r'\n', ' ', para)

        para = re.sub(r'  +', ' ', para)
        para = para.strip()
        if para:
            joined.append(para)
    text = '\n\n'.join(joined)

    text = text.strip()

    return text


def process_file(input_path: str, output_path: str) -> None:
    with open(input_path, 'r', encoding='utf-8') as f:
        content = f.read()

    changed = 0

    def replace_triple(m):
        nonlocal changed
        inner = m.group(1)
        cleaned = clean_text(inner)
        if cleaned != inner:
            changed += 1
        return '"""' + cleaned + '"""'

    def replace_single(m):
        nonlocal changed
        inner = m.group(1)
        cleaned = clean_text(inner)
        if cleaned != inner:
            changed += 1
    
        if '\n' in cleaned:
            return '"""' + cleaned + '"""'
        return '"' + cleaned + '"'

    content = re.sub(r'"""(.*?)"""', replace_triple, content, flags=re.DOTALL)

    
    def replace_single_match(m):
        nonlocal changed
        prefix = m.group(1)
        inner = m.group(2)
        cleaned = clean_text(inner)
        if cleaned != inner:
            changed += 1
        if '\n' in cleaned:
            return prefix + '"""' + cleaned + '"""'
        return prefix + '"' + cleaned + '"'

    content = re.sub(
        r'(dct:description\s+)"((?:[^"\\]|\\.)*)"',
        replace_single_match,
        content
    )

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"Done. {changed} description(s) cleaned.")


if __name__ == "__main__":
    process_file(INPUT, OUTPUT)

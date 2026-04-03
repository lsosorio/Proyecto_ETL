"""
Script para convertir README.md a PDF.
Uso: python generar_pdf.py
"""
import os
import re
import markdown
from xhtml2pdf import pisa

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ruta_md  = os.path.join(BASE_DIR, "../../README.md")
ruta_pdf = os.path.join(BASE_DIR, "../../README.pdf")


def sanitizar_unicode_para_pdf(texto: str) -> str:
    """
    Reemplaza caracteres Unicode de dibujo de cajas y flechas
    por equivalentes ASCII que las fuentes estándar del PDF soportan.
    """
    reemplazos = {
        # Esquinas
        "┌": "+", "┐": "+", "└": "+", "┘": "+",
        # Líneas
        "─": "-", "│": "|",
        # Cruces / T
        "├": "+", "┤": "+", "┬": "+", "┴": "+", "┼": "+",
        # Flechas
        "▼": "v", "▲": "^", "►": ">", "◄": "<",
        "→": "->", "←": "<-", "↓": "v", "↑": "^",
        # Emojis comunes en READMEs
        "📊": "[PyG]",
        # Checks / warnings
        "✅": "[OK]", "⚠️": "[!]", "❌": "[X]",
    }
    for viejo, nuevo in reemplazos.items():
        texto = texto.replace(viejo, nuevo)
    return texto


# --- Leer Markdown ---
with open(ruta_md, "r", encoding="utf-8") as f:
    md_content = f.read()

# --- Sanitizar caracteres Unicode no soportados ---
md_content = sanitizar_unicode_para_pdf(md_content)

# --- Convertir Markdown a HTML ---
html_body = markdown.markdown(
    md_content,
    extensions=["tables", "fenced_code", "codehilite", "toc"],
)

# --- Envolver en HTML completo con estilos ---
full_html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
    @page {{
        size: letter;
        margin: 2cm;
    }}
    body {{
        font-family: Helvetica, Arial, sans-serif;
        font-size: 11px;
        line-height: 1.5;
        color: #222;
    }}
    h1 {{
        font-size: 22px;
        color: #1a1a2e;
        border-bottom: 2px solid #1a1a2e;
        padding-bottom: 6px;
    }}
    h2 {{
        font-size: 17px;
        color: #16213e;
        border-bottom: 1px solid #ccc;
        padding-bottom: 4px;
        margin-top: 24px;
    }}
    h3 {{
        font-size: 14px;
        color: #0f3460;
        margin-top: 18px;
    }}
    table {{
        border-collapse: collapse;
        width: 100%;
        margin: 12px 0;
        font-size: 10px;
    }}
    th, td {{
        border: 1px solid #bbb;
        padding: 6px 8px;
        text-align: left;
    }}
    th {{
        background-color: #1a1a2e;
        color: #fff;
        font-weight: bold;
    }}
    tr:nth-child(even) {{
        background-color: #f4f4f8;
    }}
    pre {{
        background-color: #f5f5f5;
        border: 1px solid #ddd;
        border-radius: 4px;
        padding: 10px;
        font-size: 9px;
        overflow-x: auto;
        white-space: pre-wrap;
        word-wrap: break-word;
    }}
    code {{
        background-color: #f0f0f0;
        padding: 1px 4px;
        border-radius: 3px;
        font-size: 10px;
        font-family: "Courier New", monospace;
    }}
    pre code {{
        background-color: transparent;
        padding: 0;
    }}
    blockquote {{
        border-left: 3px solid #0f3460;
        padding: 8px 12px;
        margin: 12px 0;
        background-color: #f8f9fa;
        color: #444;
        font-style: italic;
    }}
    strong {{
        color: #1a1a2e;
    }}
    ul, ol {{
        margin: 8px 0;
        padding-left: 24px;
    }}
    li {{
        margin-bottom: 4px;
    }}
    hr {{
        border: none;
        border-top: 1px solid #ccc;
        margin: 20px 0;
    }}
    em {{
        color: #555;
    }}
</style>
</head>
<body>
{html_body}
</body>
</html>
"""

# --- Generar PDF ---
with open(ruta_pdf, "wb") as pdf_file:
    status = pisa.CreatePDF(full_html, dest=pdf_file)

if status.err:
    print(f"ERROR al generar el PDF: {status.err}")
else:
    print(f"PDF generado exitosamente: {ruta_pdf}")


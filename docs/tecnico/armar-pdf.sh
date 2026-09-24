#!/usr/bin/env bash
# Genera docs/tecnico/documento-tecnico.pdf a partir de documento.md.
#
#   documento.md  --pandoc-->  cuerpo.typ  --+
#   plantilla.typ ------------------------- +--typst--> PDF
#
# Requiere pandoc y typst. Avisa qué secciones siguen marcadas como pendientes.
set -euo pipefail
cd "$(dirname "$0")"

pandoc documento.md -t typst -o cuerpo.typ

python3 - <<'PY'
import pathlib, re
c = pathlib.Path("cuerpo.typ").read_text()
# La portada de la plantilla ya pone título y autoría.
c = re.sub(r"^= .*?\n<[^>]*>\n(?:.*?\n)*?\n", "", c, count=1)
# Neutralizar los estilos que pandoc fija por tabla, para que manden los de la plantilla.
c = re.sub(r"^  align: \(col, row\).*\n", "", c, flags=re.M)
c = re.sub(r"^  inset: 6pt,\n", "", c, flags=re.M)
c = c.replace("align(center)[#table(", "[#table(")
c = c.replace("\\|", "|")
# El diagrama es apaisado y ancho: pandoc le pone un ancho fijo que desborda la página.
# Se lo reemplaza por una página horizontal propia, que es como se lee un diagrama de este
# tipo sin encogerlo hasta que el texto no se distinga.
c = re.sub(
    r"#figure\(\[#box\(width: [\d.]+pt, image\(\"\.\./diagramas/arquitectura\.svg\"\)\);\],[^)]*\)",
    """#page(flipped: true, margin: (x: 1.1cm, y: 1.2cm))[
  #figure(
    image("/diagramas/arquitectura.svg", width: 100%),
    caption: [Arquitectura del sistema: concentrador, log crudo, pipeline, log derivado y consumidores.],
  )
]""",
    c,
)
pathlib.Path("cuerpo.typ").write_text(c)
PY

cat plantilla.typ cuerpo.typ > main.typ
# --root apunta a docs/ porque el diagrama vive en docs/diagramas/ y typst no deja
# salir del directorio del archivo que compila.
typst compile --root .. main.typ documento-tecnico.pdf
rm -f cuerpo.typ main.typ

paginas=$(pdfinfo documento-tecnico.pdf 2>/dev/null | awk '/^Pages/{print $2}')
pendientes=$(grep -c "⚠️ PENDIENTE" documento.md || true)
echo "documento-tecnico.pdf — ${paginas} páginas · ${pendientes} secciones pendientes"

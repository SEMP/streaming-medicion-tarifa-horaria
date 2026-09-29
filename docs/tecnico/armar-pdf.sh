#!/usr/bin/env bash
# Genera docs/tecnico/documento-tecnico.pdf a partir de documento.md.
#
#   documento.md  --pandoc-->  cuerpo.typ  --+
#   plantilla.typ ------------------------- +--typst--> PDF
#
# Requiere pandoc y typst. Avisa qué secciones siguen marcadas como pendientes.
set -euo pipefail
cd "$(dirname "$0")"

# Fecha fija para que el PDF sea REPRODUCIBLE. Sin esto, typst le estampa la hora de
# compilación y cada corrida produce bytes distintos aunque el documento no haya cambiado:
# el archivo aparece modificado en git sin que nadie lo tocara, y deja de poder verificarse
# que el PDF versionado corresponde al documento.md que está al lado.
# Se puede sobreescribir desde afuera: SOURCE_DATE_EPOCH=... ./armar-pdf.sh
export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-1759000000}"

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
pendientes=$(grep -c "Atención: PENDIENTE" documento.md || true)
echo "documento-tecnico.pdf — ${paginas} páginas · ${pendientes} secciones pendientes"

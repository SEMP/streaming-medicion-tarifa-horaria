// Plantilla del documento de la Tarea 1 — Streaming de datos y sus aplicaciones (FPUNA)

#let horizontalrule = align(center, line(length: 40%, stroke: 0.5pt + luma(180)))

#set document(
  title: "Consumo eléctrico por franja horaria, en streaming",
  author: "Sergio Morel",
)

#set page(
  paper: "a4",
  margin: (x: 1.8cm, top: 1.9cm, bottom: 1.7cm),
  footer: context [
    #set text(size: 8pt, fill: luma(110))
    #line(length: 100%, stroke: 0.4pt + luma(200))
    #v(-3pt)
    #grid(columns: (1fr, auto),
      align(left)[Proyecto integrador · Streaming de datos y sus aplicaciones],
      align(right)[#counter(page).display("1 / 1", both: true)])
  ],
)

#set text(font: "Libertinus Serif", size: 9.6pt, lang: "es", hyphenate: true)
#set par(justify: true, leading: 0.58em, spacing: 0.78em)

#show heading: set text(font: "Liberation Sans")
#show heading.where(level: 1): set text(size: 15pt)
#show heading.where(level: 2): it => block(above: 1.4em, below: 0.75em)[
  #set text(size: 11.5pt, fill: rgb("#1a3d5c"))
  #it.body
]
#show heading.where(level: 3): it => block(above: 1.1em, below: 0.55em)[
  #set text(size: 10pt, style: "italic")
  #it.body
]

// Código en línea y bloques
#show raw.where(block: false): it => text(
  font: "DejaVu Sans Mono", size: 0.88em, fill: rgb("#0b3d5c"), it,
)
#show raw.where(block: true): it => block(
  fill: luma(247), inset: 7pt, radius: 2pt, width: 100%,
  text(font: "DejaVu Sans Mono", size: 8pt, it),
)

// Tablas: compactas, con encabezado sombreado y sin líneas verticales
#set table(
  inset: (x: 5pt, y: 3.4pt),
  stroke: (x, y) => (
    top: if y == 0 { 0.7pt + luma(90) } else if y == 1 { 0.5pt + luma(150) } else { 0.3pt + luma(215) },
    bottom: 0pt,
  ),
  fill: (x, y) => if y == 0 { luma(238) },
)
#show table: set text(size: 8.6pt)
#show table: set par(justify: false, leading: 0.5em)
#set table(align: left + horizon)
#show table.cell.where(y: 0): it => align(left, strong(it))

// Las tablas que pandoc envuelve en #figure no necesitan numeración ni centrado extra
#show figure.caption: it => text(size: 8.2pt, style: "italic", fill: luma(95), it.body)
#show figure: it => block(width: 100%, above: 1.1em, below: 1.1em)[
  #it.body
  #if it.caption != none { v(3pt); it.caption }
]

#show link: set text(fill: rgb("#1a5c8a"))

// ---- Portada compacta (no una página entera: el documento debe ser breve) ----
#block(width: 100%, inset: 0pt)[
  #text(font: "Liberation Sans", size: 16pt, weight: "bold")[
    Consumo eléctrico por franja horaria, en streaming
  ]
  #v(4pt)
  #text(size: 10pt, fill: luma(70))[
    Un pipeline end-to-end con Apache Kafka y Apache Beam sobre lecturas
    desordenadas, duplicadas y tardías
  ]
  #v(5pt)
  #text(size: 9.5pt, fill: luma(90))[
    *Proyecto integrador* · Streaming de datos y sus aplicaciones \
    Maestría en Inteligencia Artificial, Facultad Politécnica – UNA · Prof. Rodrigo Parra, M.Sc. \
    Sergio Morel · Clara Almirón · Daniel Ramírez — septiembre de 2026
  ]
  #v(2pt)
  #line(length: 100%, stroke: 0.8pt + luma(120))
]
#v(6pt)

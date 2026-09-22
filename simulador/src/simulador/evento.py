"""Construcción del evento que se publica al tópico de lecturas crudas."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

SCHEMA_VERSION = 1
OBIS_ENERGIA_ABSOLUTA = "15.8.0"
CONTENT_TYPE = "application/json"


def calcular_event_id(medidor_id: str, instante_lectura: str) -> str:
    """Identidad estable y determinista de una lectura.

    Que sea determinista es lo que hace **reconocible al duplicado**: si el concentrador
    reintenta la publicación, el mismo hecho produce el mismo id. Con un identificador
    aleatorio, un reintento parecería un hecho nuevo y no habría forma de deduplicar.
    """
    material = f"{medidor_id}|{instante_lectura}".encode()
    return hashlib.sha256(material).hexdigest()[:16]


@dataclass(frozen=True)
class Registro:
    obis: str
    valor: float
    unidad: str
    naturaleza: str = "acumulado"
    """`acumulado` | `instantaneo` | `maximo`.

    Lo exige el contrato, y no es redundante con el código OBIS por una razón práctica: el
    pipeline **solo puede diferenciar los acumulados** —restar dos corrientes instantáneas
    no significa nada—, y deducirlo del código obligaría a mantener una tabla dentro del
    pipeline que se desactualiza sin que nadie se entere."""
    instante: str | None = None
    """Instante propio del registro, cuando lo trae.

    Algunos registros informan cuándo ocurrió *su* valor —la demanda máxima es el caso
    típico—. `15.8.0` no lo usa; el campo existe para no tener que cambiar de versión de
    esquema el día que aparezca uno que sí."""

    def a_dict(self) -> dict:
        d = {
            "obis": self.obis,
            "naturaleza": self.naturaleza,
            "valor": self.valor,
            "unidad": self.unidad,
        }
        if self.instante is not None:
            d["instante"] = self.instante
        return d


@dataclass(frozen=True)
class Lectura:
    """Una respuesta de un medidor, tal como la publica el concentrador."""

    medidor_id: str
    cabina_id: str
    lote_id: str
    secuencia: int
    instante_lectura: datetime
    """Tiempo de evento. Lo pone el **concentrador** al recibir la respuesta: el readout no
    trae timestamp propio."""
    registros: tuple[Registro, ...]
    calidad: str
    publicado_at: datetime
    """Cuándo entró al tópico. Contra `instante_lectura` da el retraso de publicación."""

    def a_dict(self) -> dict:
        instante = self.instante_lectura.isoformat(timespec="seconds")
        return {
            "schema_version": SCHEMA_VERSION,
            "event_id": calcular_event_id(self.medidor_id, instante),
            "medidor_id": self.medidor_id,
            "cabina_id": self.cabina_id,
            "lote_id": self.lote_id,
            "secuencia": self.secuencia,
            "instante_lectura": instante,
            "registros": [r.a_dict() for r in self.registros],
            "calidad": self.calidad,
            "publicado_at": self.publicado_at.isoformat(timespec="milliseconds"),
        }

    def a_json(self) -> str:
        return json.dumps(self.a_dict(), ensure_ascii=False)

    def headers(self) -> list[tuple[str, bytes]]:
        """Headers de Kafka, que el contrato exige.

        Permiten a un consumidor **enrutar o rechazar sin deserializar el cuerpo**: leer
        un header es mucho más barato que parsear un JSON, y en cuarentena o en una
        migración de esquema esa diferencia importa."""
        instante = self.instante_lectura.isoformat(timespec="seconds")
        return [
            ("schema_version", str(SCHEMA_VERSION).encode()),
            ("content_type", CONTENT_TYPE.encode()),
            # Determinista, igual que el event_id: dos corridas iguales trazan igual.
            ("trace_id", calcular_event_id(self.lote_id, instante).encode()),
        ]

    @property
    def clave(self) -> str:
        """Clave de particionamiento: todas las lecturas de un medidor a la misma partición,
        para que su orden se preserve y el diferenciado pueda restar consecutivas."""
        return self.medidor_id

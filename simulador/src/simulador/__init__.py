"""Simulador de lectura remota de medidores eléctricos.

Genera lecturas sintéticas del registro acumulado 15.8.0 tal como las obtendría un
concentrador que consulta medidores agrupados en cabinas sobre un bus RS-485.

Existe para **inyectar fallas a propósito**: pedidos que se corren, pedidos que fallan,
cabinas que se caen enteras, tramas truncadas, duplicados y ráfagas tardías. Todo
configurable y determinista — la misma semilla produce exactamente los mismos fallos.
"""

__all__ = ["consumo", "parque", "evento", "agenda", "fallas"]

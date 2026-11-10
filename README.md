# Radar de noticias IA (local)

Sitio local que lee en vivo los RSS de 10 medios y muestra las noticias de IA en una sola página.

## Cómo usarlo

```bash
python3 radar.py
```

Se abre solo en http://localhost:8765. Deja la terminal abierta mientras lo usas (Ctrl+C para detenerlo).

- **Actualizar ahora** vuelve a leer todos los feeds. La página también se refresca sola cada 15 minutos.
- Filtra por tema, por fuente o busca una palabra (ej. "Oracle").
- La columna **Fuentes** muestra en verde los feeds que respondieron y en rojo los que fallaron, con el motivo.

Opciones: `python3 radar.py --port 9000 --no-browser`

## Agregar o quitar fuentes

Edita la lista `FEEDS` al inicio de `radar.py`. Cada fuente lleva `name`, `url` (el RSS) y `site`.
Usa `"ai_only": True` si el feed es de tecnología general y solo quieres las notas de IA.

Truco para medios sin RSS: Google News genera uno con cualquier búsqueda, por ejemplo
`https://news.google.com/rss/search?q=AI+site:reuters.com+when:3d&hl=en-US&gl=US&ceid=US:en`

## Si los feeds salen en rojo con "Certificados SSL faltantes" (macOS)

El Python de python.org en Mac no trae certificados. Una de estas dos lo arregla:

```bash
open "/Applications/Python 3.12/Install Certificates.command"   # ajusta tu versión
# o bien
pip3 install certifi
```

## Que arranque solo (opcional)

- **macOS / Linux:** agrega a tu crontab `@reboot cd /ruta/a/radar-ia && python3 radar.py --no-browser`
- **Windows:** crea una tarea en el Programador de tareas que ejecute `pythonw radar.py --no-browser` al iniciar sesión.

Después solo guarda http://localhost:8765 en tus favoritos.

## Publicarlo en GitHub Pages (para verlo en el iPad o el celular)

`python3 radar.py --static _site/index.html` genera una sola página con las noticias incrustadas.
El archivo `.github/workflows/publicar.yml` hace eso cada hora en GitHub y publica el resultado en GitHub Pages.
Ojo: GitHub pausa las tareas programadas de un repositorio público si pasan 60 días sin cambios;
si deja de actualizarse, entra a la pestaña Actions y vuelve a activarla.

#!/usr/bin/env python3
"""
spotify de pobre - guarda arquivos em nomes de playlists do Spotify
"""
from __future__ import annotations

import os
import re
import sys
import json
import stat
import time
import zlib
import base64
import hashlib
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from pathlib import Path

PLAYLIST_MAX   = 100
PREFIX         = "spdb"
META_PREFIX    = "spdm"
SCOPE          = "playlist-modify-private playlist-read-private"
CONFIG_FILE    = Path.home() / ".spotifydepobre" / "config.json"
STATE_DIR      = CONFIG_FILE.parent
RETRY_ATTEMPTS = 5
RETRY_AFTER_MAX = 60
SHA256_LEN     = 64
STATE_SCHEMA    = 2
WARN_PLAYLISTS  = int(os.environ.get("SPDB_WARN_PLAYLISTS", "500"))
FORCE_YES       = os.environ.get("SPDB_YES", "").lower() in ("1", "true", "yes")
CONSEC_FAIL_MAX = int(os.environ.get("SPDB_CONSEC_FAIL_MAX", "10"))

# nomes reservados do Windows que não podem ser usados como storage_name
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *[f"COM{i}" for i in range(1, 10)],
    *[f"LPT{i}" for i in range(1, 10)],
}


def _confirm(prompt: str) -> bool:
    """Pede confirmação. Retorna True automaticamente se SPDB_YES=1."""
    if FORCE_YES:
        print(f"{prompt} [auto-sim]")
        return True
    return input(f"{prompt} [s/N] ").strip().lower() == "s"


# ── config / login ────────────────────────────────────────────────────────────

def load_config() -> dict:
    if CONFIG_FILE.exists():
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    return {}


def save_config(cfg: dict):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(cfg, indent=2), encoding="utf-8")


def login():
    print("\n=== Login no Spotify ===")
    print("Precisas de uma app no Spotify Developer Dashboard.")
    print("Acessa: https://developer.spotify.com/dashboard")
    print("Cria uma app e adiciona  http://localhost:8888/callback  como Redirect URI.\n")

    cfg = load_config()
    client_id     = input(f"Client ID     [{cfg.get('client_id', '')}]: ").strip() or cfg.get("client_id", "")
    client_secret = input(f"Client Secret [{cfg.get('client_secret', '')}]: ").strip() or cfg.get("client_secret", "")

    if not client_id or not client_secret:
        print("Client ID e Client Secret são obrigatórios.")
        sys.exit(1)

    cfg.update({"client_id": client_id, "client_secret": client_secret})
    save_config(cfg)
    _make_spotify(client_id, client_secret)
    print("\nLogin efetuado com sucesso!")


def _make_spotify(client_id=None, client_secret=None) -> spotipy.Spotify:
    cfg           = load_config()
    client_id     = client_id     or cfg.get("client_id")
    client_secret = client_secret or cfg.get("client_secret")

    if not client_id or not client_secret:
        print("Ainda não fizeste login. Corre:  python main.py login")
        sys.exit(1)

    cache_path = str(STATE_DIR / ".cache")
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri="http://localhost:8888/callback",
        scope=SCOPE,
        cache_path=cache_path,
        open_browser=True,
    ))

    # protege a directoria com 0o700 — o spotipy pode recriar .cache com perms default;
    # proteger a directoria garante que só o dono acede independentemente do ficheiro
    try:
        STATE_DIR.chmod(stat.S_IRWXU)
    except Exception:
        pass  # pode falhar em Windows ou filesystems sem suporte

    return sp


# ── helpers ───────────────────────────────────────────────────────────────────

def _progress(current: int, total: int, label: str = ""):
    pct = current / total
    bar = ("█" * int(pct * 30)).ljust(30)
    print(f"\r  [{bar}] {current}/{total} {label}   ", end="", flush=True)


def _api_call(fn, *args, _consec_fails: list | None = None, **kwargs):
    """Chama fn com retry e backoff. Preserva a exceção original após esgotar tentativas.
    _consec_fails é lista mutável partilhada pelo chamador para circuit breaker global.
    Só conta erros 429/5xx — 404/403 não incrementam o circuit breaker.
    """
    last_exc = None
    for attempt in range(RETRY_ATTEMPTS):
        try:
            result = fn(*args, **kwargs)
            if _consec_fails is not None:
                _consec_fails.clear()  # reset após qualquer sucesso
            return result
        except spotipy.SpotifyException as e:
            last_exc = e
            if e.http_status == 429:
                if _consec_fails is not None:
                    _consec_fails.append(1)
                    if len(_consec_fails) >= CONSEC_FAIL_MAX:
                        raise RuntimeError(
                            f"Circuit breaker: {CONSEC_FAIL_MAX} falhas 429 consecutivas. "
                            "O Spotify pode estar instável. Tenta mais tarde."
                        ) from e
                wait = min(2 ** attempt, RETRY_AFTER_MAX)
                print(f"\n  Rate limit. Aguardando {wait}s ({attempt+1}/{RETRY_ATTEMPTS})...", flush=True)
                time.sleep(max(0, wait))  # time.sleep com valor negativo faz sleep(0) — garantir >= 0
            else:
                raise  # 401, 403, 404, 500 — não faz retry, não incrementa circuit breaker
    raise last_exc  # type: ignore


def _all_user_playlists(sp: spotipy.Spotify, user_id: str) -> list:
    playlists, results = [], _api_call(sp.user_playlists, user_id, limit=50)
    while results:
        playlists.extend(results["items"])
        results = _api_call(sp.next, results) if results["next"] else None
    return playlists


SEP     = "|"
NAME_RE = re.compile(r'^[A-Za-z0-9._-]+$')


def _safe_filename(storage_name: str) -> str:
    """Sanitiza para uso em paths — substitui chars problemáticos por _."""
    return re.sub(r'[^A-Za-z0-9._-]', '_', storage_name)[:64]


def _validate_name(storage_name: str):
    if not storage_name or not storage_name.strip():
        print("Nome não pode ser vazio.")
        sys.exit(1)
    if not NAME_RE.match(storage_name):
        print("Nome só pode conter letras, números, '.', '_' e '-'.")
        sys.exit(1)
    # nomes reservados do Windows falham silenciosamente em ficheiros de estado/lock
    if storage_name.upper() in _WINDOWS_RESERVED:
        print(f"Nome '{storage_name}' é reservado pelo Windows. Escolhe outro nome.")
        sys.exit(1)
    if _chunk_size(storage_name) <= 0:
        print(f"Nome '{storage_name}' é longo demais. Usa um nome mais curto.")
        sys.exit(1)


def _chunk_size(storage_name: str) -> int:
    overhead = len(f"{PREFIX}{SEP}{storage_name}{SEP}000000{SEP}000000{SEP}")
    return PLAYLIST_MAX - overhead


def _playlist_name(storage_name: str, index: int, total: int, chunk: str) -> str:
    return f"{PREFIX}{SEP}{storage_name}{SEP}{index:06d}{SEP}{total:06d}{SEP}{chunk}"


def _parse_playlist(name: str):
    if not name.startswith(f"{PREFIX}{SEP}"):
        return None
    parts = name.split(SEP, 4)
    if len(parts) != 5:
        return None
    try:
        return parts[1], int(parts[2]), int(parts[3]), parts[4]
    except ValueError:
        return None


def _parse_meta(name: str):
    if not name.startswith(f"{META_PREFIX}{SEP}"):
        return None
    parts = name.split(SEP, 3)
    if len(parts) != 4:
        return None
    sname, checksum, ext = parts[1], parts[2], parts[3]
    if len(checksum) != SHA256_LEN or not all(c in "0123456789abcdef" for c in checksum):
        return None
    return sname, checksum, ext


# ── estado de rollback ────────────────────────────────────────────────────────

def _state_path(storage_name: str) -> Path:
    return STATE_DIR / f".state.{_safe_filename(storage_name)}.json"


def _lock_path(storage_name: str) -> Path:
    return STATE_DIR / f".lock.{_safe_filename(storage_name)}"


def _write_state(path: Path, data: dict):
    """Escrita atómica do estado — funciona em Windows e Unix."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data), encoding="utf-8")
    try:
        os.replace(tmp, path)
    except PermissionError:
        # Windows NTFS: os.replace falha se o destino estiver aberto.
        # Pausa curta antes de tentar novamente — evita apagar o destino
        # (que criaria janela de perda de estado entre unlink e replace).
        time.sleep(0.1)
        try:
            os.replace(tmp, path)
        except PermissionError:
            # último recurso: janela de perda mínima (< 1ms em disco local)
            path.unlink(missing_ok=True)
            os.replace(tmp, path)


def _read_state(path: Path) -> dict | None:
    """Lê estado e valida schema. Avisa se versão incompatível em vez de descartar silenciosamente."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    schema = data.get("schema")
    if schema != STATE_SCHEMA:
        name = data.get("name", "?")
        print(f"Aviso: estado de versão {schema} encontrado para '{name}' (atual: {STATE_SCHEMA}).")
        print(f"  Pode haver playlists incompletas no Spotify. Corre: python main.py delete {name}")
        return None
    return data


def _acquire_lock(storage_name: str) -> Path:
    """Lock com PID — detecta processos mortos e liberta o lock automaticamente."""
    lock = _lock_path(storage_name)
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    if lock.exists():
        try:
            pid = int(lock.read_text(encoding="utf-8").strip())
            os.kill(pid, 0)
            # sem exceção: processo existe — pode ser nosso ou de outro utilizador
            # PermissionError é tratada em separado abaixo, por isso chegamos aqui
            # apenas quando o processo existe e pertence ao mesmo utilizador
            print(f"Outro upload de '{storage_name}' está em curso (PID {pid}).")
            print("Se o processo já não existe, apaga o lock manualmente:")
            print(f"  {lock}")
            sys.exit(1)
        except PermissionError:
            # processo existe mas pertence a outro utilizador — nunca expiramos
            print(f"Lock de '{storage_name}' pertence a outro utilizador (PID no lock).")
            print(f"Apaga manualmente se necessário: {lock}")
            sys.exit(1)
        except ProcessLookupError:
            # processo não existe — lock expirado, pode reutilizar
            lock.unlink(missing_ok=True)
        except (ValueError, OSError):
            # PID inválido ou ficheiro corrompido — descarta
            lock.unlink(missing_ok=True)

    try:
        lock.open("x").close()
        # fsync do PID para garantir que chega ao disco antes de um crash
        with lock.open("w") as f:
            f.write(str(os.getpid()))
            f.flush()
            os.fsync(f.fileno())
    except FileExistsError:
        print(f"Conflito ao criar lock para '{storage_name}'. Tenta novamente.")
        sys.exit(1)

    return lock


# ── comandos ──────────────────────────────────────────────────────────────────

def upload(filepath: str, storage_name: str):
    _validate_name(storage_name)
    path = Path(filepath).resolve()
    if not path.exists():
        print(f"Arquivo não encontrado: {filepath}")
        sys.exit(1)
    if not path.is_file():
        print(f"'{filepath}' não é um arquivo.")
        sys.exit(1)

    sp      = _make_spotify()
    user_id = sp.me()["id"]

    # verifica se já existe
    all_pl   = _all_user_playlists(sp, user_id)
    existing = [p for p in all_pl if
                (_parse_playlist(p["name"]) and _parse_playlist(p["name"])[0] == storage_name) or
                (_parse_meta(p["name"])     and _parse_meta(p["name"])[0]     == storage_name)]

    if existing:
        if not _confirm(f"'{storage_name}' já existe ({len(existing)} playlists). Sobrescrever?"):
            print("Cancelado.")
            return
        print("Deletando versão anterior...")
        for p in existing:
            try:
                _api_call(sp.current_user_unfollow_playlist, p["id"])
            except spotipy.SpotifyException as e:
                if e.http_status != 404:
                    raise

    raw        = path.read_bytes()
    checksum   = hashlib.sha256(raw).hexdigest()
    compressed = zlib.compress(raw, level=9)
    use_zip    = len(compressed) < len(raw)
    payload    = compressed if use_zip else raw
    data       = base64.urlsafe_b64encode(payload).decode()

    cs     = _chunk_size(storage_name)
    ext    = path.suffix.lstrip(".")[:8]  # sem ponto, máx 8 chars
    chunks = [data[i:i+cs] for i in range(0, len(data), cs)]
    total  = len(chunks)

    meta_name = f"{META_PREFIX}{SEP}{storage_name}{SEP}{checksum}{SEP}{ext}"
    if len(meta_name) > PLAYLIST_MAX:
        print(f"Erro interno: meta_name excede {PLAYLIST_MAX} chars ({len(meta_name)}). Reporta este bug.")
        sys.exit(1)

    # avisa se o número de playlists for grande
    if total > WARN_PLAYLISTS:
        if not _confirm(f"Este upload vai criar {total} playlists. Continuar?"):
            print("Cancelado.")
            return

    ratio = f" (comprimido {len(raw):,} → {len(payload):,} bytes)" if use_zip else ""
    print(f"\nArquivo : {path.name}  ({path.stat().st_size:,} bytes){ratio}")
    print(f"Playlists: {total}  |  {cs} chars/playlist\n")

    lock         = _acquire_lock(storage_name)
    state_file   = _state_path(storage_name)
    created: list[str] = []
    consec_fails: list = []  # circuit breaker partilhado por todas as chamadas do upload

    def _api(fn, *args, **kwargs):
        return _api_call(fn, *args, _consec_fails=consec_fails, **kwargs)

    def _save_state(completed: bool = False):
        _write_state(state_file, {
            "schema":    STATE_SCHEMA,
            "name":      storage_name,
            "ids":       created,
            "completed": completed,
        })

    def _rollback(reason: str):
        print(f"\n\n{reason}")
        print(f"Limpando {len(created)} playlists criadas...")
        for pl_id in created:
            try:
                _api_call(sp.current_user_unfollow_playlist, pl_id)
            except spotipy.SpotifyException as e:
                if e.http_status != 404:
                    pass
        state_file.unlink(missing_ok=True)
        lock.unlink(missing_ok=True)
        print("Rollback completo.")

    # retoma rollback de upload anterior interrompido
    prev = _read_state(state_file)
    if prev and prev.get("name") == storage_name and not prev.get("completed", False):
        prev_ids = prev.get("ids", [])
        if prev_ids:
            print(f"Upload anterior de '{storage_name}' foi interrompido. A limpar {len(prev_ids)} playlists...")
            for pl_id in prev_ids:
                try:
                    _api_call(sp.current_user_unfollow_playlist, pl_id)
                except spotipy.SpotifyException as e:
                    if e.http_status != 404:
                        pass
    state_file.unlink(missing_ok=True)

    try:
        for i, chunk in enumerate(chunks):
            pl_name = _playlist_name(storage_name, i, total, chunk)
            p = _api(sp.user_playlist_create, user_id, pl_name, public=False)
            created.append(p["id"])
            _save_state()
            _progress(i + 1, total, "uploading")

        p = _api(sp.user_playlist_create, user_id, meta_name, public=False)
        created.append(p["id"])
        _save_state(completed=True)

    except (KeyboardInterrupt, Exception) as e:
        _rollback(f"Erro durante upload: {e}")
        sys.exit(1)

    state_file.unlink(missing_ok=True)
    lock.unlink(missing_ok=True)
    print(f"\n\nPronto! '{storage_name}' guardado em {total} playlists. SHA256: {checksum[:12]}...")


def download(storage_name: str, output_path: str):
    _validate_name(storage_name)
    sp      = _make_spotify()
    user_id = sp.me()["id"]

    print(f"Buscando '{storage_name}'...")
    entries        = {}  # index → chunk (dict detecta duplicados)
    expected_total = 0
    checksum       = None
    original_ext   = ""

    for p in _all_user_playlists(sp, user_id):
        parsed = _parse_playlist(p["name"])
        if parsed and parsed[0] == storage_name:
            idx = parsed[1]
            if idx in entries:
                print(f"Aviso: chunk duplicado no índice {idx} — usando o primeiro encontrado.")
            else:
                entries[idx] = parsed[3]
            expected_total = parsed[2]
            continue
        meta = _parse_meta(p["name"])
        if meta and meta[0] == storage_name:
            checksum = meta[1]
            # sanitiza extensão antes de usar em path — remove tudo exceto alnum
            raw_ext      = re.sub(r'[^A-Za-z0-9]', '', meta[2])[:8]
            original_ext = f".{raw_ext}" if raw_ext else ""

    if not entries:
        print("Nenhum arquivo encontrado com esse nome.")
        sys.exit(1)

    if len(entries) != expected_total:
        print(f"Aviso: esperadas {expected_total} playlists, encontradas {len(entries)}. O arquivo pode estar corrompido.")

    # verifica que não há índices em falta na sequência
    missing = [i for i in range(expected_total) if i not in entries]
    if missing:
        print(f"Aviso: {len(missing)} chunks em falta: {missing[:10]}{'...' if len(missing) > 10 else ''}")

    total = len(entries)
    print(f"Encontradas {total} playlists. Reconstruindo...\n")

    chunks = []
    for i, (_, chunk) in enumerate(sorted(entries.items())):
        chunks.append(chunk)
        _progress(i + 1, total, "downloading")

    out = Path(output_path)
    # trata extensões compostas (.tar.gz, .tar.bz2) — Path.suffix só devolve a última
    # se o output_path não tem extensão e temos a original, usa-a
    if not out.suffix and original_ext:
        out = out.with_suffix(original_ext)
    elif not out.suffix and not original_ext:
        pass  # sem extensão conhecida — deixa como está

    joined = "".join(chunks)
    padding = "=" * ((4 - len(joined) % 4) % 4)
    # tenta urlsafe primeiro; fallback para standard (ficheiros de versões antigas do script)
    try:
        raw = base64.urlsafe_b64decode(joined + padding)
    except Exception:
        try:
            raw = base64.b64decode(joined + padding)
            print("  Aviso: ficheiro codificado com versão antiga do script (Base64 standard).")
        except Exception as e:
            print(f"\n\nERRO: não foi possível decodificar os dados: {e}")
            sys.exit(1)
    try:
        raw = zlib.decompress(raw)
    except zlib.error:
        pass

    if checksum:
        actual = hashlib.sha256(raw).hexdigest()
        if actual != checksum:
            print(f"\n\nERRO: checksum não bate! Arquivo corrompido.")
            print(f"  Esperado : {checksum}")
            print(f"  Calculado: {actual}")
            if missing:
                print(f"  Causa provável: {len(missing)} chunks em falta.")
            sys.exit(1)
        print(f"\n  Integridade OK (SHA256: {checksum[:12]}...)")
    else:
        print("\n  Aviso: sem metadados de checksum (arquivo antigo).")

    out.parent.mkdir(parents=True, exist_ok=True)
    # verifica espaço em disco antes de escrever — usa tamanho descomprimido (raw), não Base64
    try:
        st   = os.statvfs(out.parent)
        free = st.f_bavail * st.f_frsize
        if len(raw) > free:
            print(f"\nERRO: espaço insuficiente. Necessário: {len(raw):,} B, disponível: {free:,} B")
            sys.exit(1)
    except AttributeError:
        pass  # os.statvfs não existe em Windows
    # SPDB_YES não salta confirmação de sobrescrita de ficheiro de output — perda de dados local
    if out.exists():
        if not input(f"'{out}' já existe. Sobrescrever? [s/N] ").strip().lower() == "s":
            print("Cancelado.")
            sys.exit(0)
    out.write_bytes(raw)
    print(f"Arquivo salvo em: {out.resolve()}")


def list_files():
    sp      = _make_spotify()
    user_id = sp.me()["id"]

    print("Buscando arquivos guardados...\n")
    all_pl = _all_user_playlists(sp, user_id)

    counts:    dict[str, int] = {}
    totals:    dict[str, int] = {}
    checksums: dict[str, str] = {}
    for p in all_pl:
        parsed = _parse_playlist(p["name"])
        if parsed:
            sname = parsed[0]
            counts[sname] = counts.get(sname, 0) + 1
            totals[sname] = parsed[2]
            continue
        meta = _parse_meta(p["name"])
        if meta:
            checksums[meta[0]] = meta[1]

    # inclui ficheiros que só têm metadados (dados apagados manualmente)
    all_names = set(counts) | set(checksums)
    if not all_names:
        print("Nenhum arquivo guardado.")
        return

    print(f"{'Nome':<30} {'Partes':>8}  {'Estado':<22}  {'Tamanho aprox.':>14}")
    print("-" * 78)
    for name in sorted(all_names):
        count    = counts.get(name, 0)
        declared = totals.get(name, 0)
        cs       = _chunk_size(name)
        approx   = count * cs * 3 // 4
        has_meta = "✓" if name in checksums else "sem meta"
        if count == 0:
            status = f"SÓ META ({has_meta})"
        elif count == declared:
            status = f"OK ({has_meta})"
        else:
            status = f"INCOMPLETO ({count}/{declared})"
        print(f"{name:<30} {count:>8}  {status:<22}  {approx:>12,} B")


def delete(storage_name: str):
    _validate_name(storage_name)
    sp      = _make_spotify()
    user_id = sp.me()["id"]

    all_pl    = _all_user_playlists(sp, user_id)
    # apaga dados E metadados independentemente — evita órfãos spdm|
    playlists = [p for p in all_pl if
                 (_parse_playlist(p["name"]) and _parse_playlist(p["name"])[0] == storage_name) or
                 (_parse_meta(p["name"])     and _parse_meta(p["name"])[0]     == storage_name)]

    if not playlists:
        print("Nenhum arquivo encontrado com esse nome.")
        return

    resp = input(f"Deletar '{storage_name}' ({len(playlists)} playlists)? [s/N] ").strip().lower()
    if resp != "s":
        print("Cancelado.")
        return

    failed = []
    for i, p in enumerate(playlists):
        try:
            _api_call(sp.current_user_unfollow_playlist, p["id"])
        except spotipy.SpotifyException as e:
            if e.http_status != 404:
                failed.append(p["name"])
        _progress(i + 1, len(playlists), "deletando")

    _state_path(storage_name).unlink(missing_ok=True)
    _lock_path(storage_name).unlink(missing_ok=True)

    print(f"\n{len(playlists) - len(failed)} playlists deletadas.")
    if failed:
        print(f"  {len(failed)} falharam — corre delete novamente para limpar o resto.")


# ── entry point ───────────────────────────────────────────────────────────────

def usage():
    print("\nUso: python main.py <comando> [args]\n")
    print("Comandos:")
    for cmd, args in [
        ("login",    ""),
        ("upload",   "<arquivo> <nome>"),
        ("download", "<nome> <saída>"),
        ("list",     ""),
        ("delete",   "<nome>"),
    ]:
        print(f"  {cmd:<10} {args}")
    print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "login":
        login()
    elif cmd == "upload" and len(sys.argv) == 4:
        upload(sys.argv[2], sys.argv[3])
    elif cmd == "download" and len(sys.argv) == 4:
        download(sys.argv[2], sys.argv[3])
    elif cmd == "list":
        list_files()
    elif cmd == "delete" and len(sys.argv) == 3:
        delete(sys.argv[2])
    else:
        usage()

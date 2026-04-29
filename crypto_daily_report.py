"""
Relatório Diário — Top Canais de Cripto no YouTube
Coleta vídeos das últimas 24h dos 5 maiores canais, calcula engajamento e envia por email via Resend.
"""

import html
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests


# ---------- Configuração ----------
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY")
RESEND_API_KEY  = os.environ.get("RESEND_API_KEY")
SENDER_EMAIL    = os.environ.get("SENDER_EMAIL", "onboarding@resend.dev")
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL", "marcusvmoraes86@gmail.com")

# Modo de execução: "check" (só envia se for hora de pico) ou "force" (envia sempre)
RUN_MODE = os.environ.get("RUN_MODE", "check")
PEAK_FILE = Path(__file__).parent / "peak_hour.txt"

# Janela de busca: últimas 24h (relatório diário) com fallback de 7 dias
DAILY_WINDOW_HOURS   = 24
FALLBACK_WINDOW_DAYS = 7
MAX_VIDEOS_PER_CHANNEL = 5

# Top 5 canais de cripto — usamos handles (@) para resolver IDs dinamicamente
TOP_CRYPTO_HANDLES = [
    "@CoinBureau",
    "@AltcoinDaily",
    "@IntoTheCryptoverse",   # Benjamin Cowen
    "@cryptobantergroup",    # Crypto Banter
    "@DataDash",
]

BASE_URL = "https://www.googleapis.com/youtube/v3"
HTTP_TIMEOUT = 15
MAX_RETRIES  = 3
RETRY_BACKOFF = 2  # segundos


# ---------- HTTP com retry ----------
def yt_get(endpoint, params):
    """GET na API do YouTube com retry e tratamento de erro."""
    params = {**params, "key": YOUTUBE_API_KEY}
    url = f"{BASE_URL}/{endpoint}"
    last_err = None

    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(RETRY_BACKOFF * (2 ** attempt))
                continue
            # 4xx que não vale retry
            print(f"[YT API] {endpoint} retornou {r.status_code}: {r.text[:200]}", file=sys.stderr)
            return None
        except requests.RequestException as e:
            last_err = e
            time.sleep(RETRY_BACKOFF * (2 ** attempt))

    print(f"[YT API] Falha após {MAX_RETRIES} tentativas em {endpoint}: {last_err}", file=sys.stderr)
    return None


# ---------- Resolução de canais ----------
def resolve_channel_by_handle(handle):
    """Resolve handle (@nome) → channelId + uploads playlistId + stats."""
    data = yt_get("channels", {
        "part": "snippet,statistics,contentDetails",
        "forHandle": handle,
    })
    if not data or not data.get("items"):
        return None
    ch = data["items"][0]
    return {
        "id":           ch["id"],
        "name":         ch["snippet"]["title"],
        "subscribers":  int(ch["statistics"].get("subscriberCount", 0)),
        "uploads_pid":  ch["contentDetails"]["relatedPlaylists"]["uploads"],
    }


def get_recent_uploads(uploads_playlist_id, max_results=10):
    """Lista vídeos do playlist de uploads (1 unidade de quota — muito mais barato que search.list)."""
    data = yt_get("playlistItems", {
        "part": "snippet,contentDetails",
        "playlistId": uploads_playlist_id,
        "maxResults": max_results,
    })
    if not data:
        return []
    items = []
    for it in data.get("items", []):
        snip = it["snippet"]
        items.append({
            "video_id":   it["contentDetails"]["videoId"],
            "title":      snip["title"],
            "published":  it["contentDetails"].get("videoPublishedAt") or snip.get("publishedAt"),
        })
    return items


def get_videos_details(video_ids):
    """Busca stats + descrição em batch (1 unidade por chamada, até 50 vídeos)."""
    if not video_ids:
        return {}
    data = yt_get("videos", {
        "part": "statistics,snippet,contentDetails",
        "id": ",".join(video_ids),
    })
    if not data:
        return {}
    out = {}
    for item in data.get("items", []):
        s = item["statistics"]
        out[item["id"]] = {
            "views":       int(s.get("viewCount",    0)),
            "likes":       int(s.get("likeCount",    0)),
            "comments":    int(s.get("commentCount", 0)),
            "description": item["snippet"].get("description", ""),
            "duration":    item["contentDetails"].get("duration", ""),
            "tags":        item["snippet"].get("tags", []),
        }
    return out


# ---------- Lógica de análise ----------
def engagement_score(views, likes, comments):
    """Score = views + (likes × 10) + (comentários × 20)."""
    return views + (likes * 10) + (comments * 20)


def filter_by_window(videos, hours):
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    out = []
    for v in videos:
        try:
            pub = datetime.fromisoformat(v["published"].replace("Z", "+00:00"))
            if pub >= cutoff:
                out.append(v)
        except (ValueError, KeyError):
            continue
    return out


def summarize_description(desc, max_chars=280):
    """Pega o início útil da descrição como 'resumo'."""
    if not desc:
        return "Sem descrição disponível."
    # Remove links/urls e limpa
    lines = [ln.strip() for ln in desc.splitlines() if ln.strip()]
    summary = " ".join(lines)
    # Corta no primeiro indicador de seções tipo "Timestamps:", "👉", links
    for marker in ["Timestamps:", "Chapters:", "Links:", "Follow", "Subscribe", "http"]:
        idx = summary.lower().find(marker.lower())
        if idx > 50:
            summary = summary[:idx]
            break
    summary = summary.strip()
    if len(summary) > max_chars:
        summary = summary[:max_chars].rsplit(" ", 1)[0] + "..."
    return summary or "Sem descrição disponível."


# ---------- Formatação ----------
def fmt(n):
    n = int(n)
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def esc(s):
    return html.escape(str(s or ""))


def build_html_report(channels_data, window_label):
    today = datetime.now().strftime("%d/%m/%Y")

    # Achata todos os vídeos para encontrar o campeão de engajamento
    all_videos = []
    for ch in channels_data:
        for v in ch["videos"]:
            v["_channel"] = ch["name"]
            all_videos.append(v)

    top_video = max(all_videos, key=lambda v: v["engagement"]) if all_videos else None

    # Seção do vídeo TOP
    top_section = ""
    if top_video:
        top_section = f"""
        <div style="background:#1f1200;border:2px solid #f0b429;border-radius:8px;padding:16px;margin-bottom:24px;">
          <h2 style="color:#f0b429;margin:0 0 8px;font-size:18px;">🏆 Vídeo com Maior Engajamento</h2>
          <p style="margin:4px 0;color:#fff;font-size:16px;font-weight:bold;">{esc(top_video['title'])}</p>
          <p style="margin:4px 0;color:#aaa;font-size:13px;">Canal: {esc(top_video['_channel'])}</p>
          <p style="margin:8px 0;color:#ddd;font-size:13px;line-height:1.5;">{esc(top_video['summary'])}</p>
          <p style="margin:4px 0;">
            <span style="color:#34d399;">👁 {fmt(top_video['views'])}</span> &nbsp;
            <span style="color:#60a5fa;">👍 {fmt(top_video['likes'])}</span> &nbsp;
            <span style="color:#f472b6;">💬 {fmt(top_video['comments'])}</span> &nbsp;
            <span style="color:#f0b429;font-weight:bold;">⚡ Score: {fmt(top_video['engagement'])}</span>
          </p>
          <a href="https://youtu.be/{esc(top_video['video_id'])}" style="color:#f0b429;text-decoration:none;font-weight:bold;">▶ Assistir agora</a>
        </div>
        """

    # Tabelas por canal
    channels_html = ""
    for ch in channels_data:
        if not ch["videos"]:
            continue
        videos_sorted = sorted(ch["videos"], key=lambda x: x["engagement"], reverse=True)
        rows = ""
        for v in videos_sorted:
            is_top = top_video and v["video_id"] == top_video["video_id"]
            highlight = "background:#2a1a00;" if is_top else ""
            badge = ' <span style="color:#f0b429;font-weight:bold;">🏆</span>' if is_top else ""
            title_short = esc(v['title'][:80] + ('...' if len(v['title']) > 80 else ''))
            rows += f"""
            <tr style="border-bottom:1px solid #2a2a4a;{highlight}">
              <td style="padding:12px;vertical-align:top;">
                <a href="https://youtu.be/{esc(v['video_id'])}" style="color:#60a5fa;text-decoration:none;font-weight:bold;">
                  {title_short}{badge}
                </a>
                <div style="font-size:11px;color:#888;margin-top:4px;">📅 {esc(v['published'][:10])}</div>
                <div style="font-size:12px;color:#bbb;margin-top:6px;line-height:1.4;">{esc(v['summary'])}</div>
              </td>
              <td style="padding:12px;text-align:center;color:#34d399;white-space:nowrap;">{fmt(v['views'])}</td>
              <td style="padding:12px;text-align:center;color:#60a5fa;white-space:nowrap;">{fmt(v['likes'])}</td>
              <td style="padding:12px;text-align:center;color:#f472b6;white-space:nowrap;">{fmt(v['comments'])}</td>
              <td style="padding:12px;text-align:center;font-weight:bold;color:#f0b429;white-space:nowrap;">{fmt(v['engagement'])}</td>
            </tr>
            """

        channels_html += f"""
        <div style="margin-top:24px;background:#16213e;border-radius:8px;overflow:hidden;">
          <div style="padding:14px 16px;background:#1a1a2e;border-bottom:2px solid #f0b429;">
            <span style="font-size:17px;font-weight:bold;color:#f0b429;">📺 {esc(ch['name'])}</span>
            <span style="font-size:12px;color:#aaa;margin-left:10px;">{fmt(ch['subscribers'])} inscritos</span>
            <span style="font-size:12px;color:#888;margin-left:10px;">{len(ch['videos'])} vídeo(s) na janela</span>
          </div>
          <table style="width:100%;border-collapse:collapse;font-size:13px;">
            <tr style="background:#0f1830;color:#999;font-size:11px;text-transform:uppercase;">
              <th style="padding:8px 12px;text-align:left;">Vídeo</th>
              <th style="padding:8px 12px;">Views</th>
              <th style="padding:8px 12px;">Likes</th>
              <th style="padding:8px 12px;">Coment.</th>
              <th style="padding:8px 12px;">Score</th>
            </tr>
            {rows}
          </table>
        </div>
        """

    if not channels_html:
        channels_html = '<p style="color:#aaa;text-align:center;padding:32px;">Nenhum vídeo encontrado na janela.</p>'

    return f"""<!DOCTYPE html>
<html><body style="background:#0f0f23;color:#e0e0e0;font-family:-apple-system,Arial,sans-serif;padding:24px;margin:0;">
  <div style="max-width:820px;margin:0 auto;">
    <h1 style="color:#f0b429;border-bottom:2px solid #f0b429;padding-bottom:10px;margin:0 0 8px;">
      🚀 Top Cripto YouTube
    </h1>
    <p style="color:#aaa;margin:0 0 20px;font-size:13px;">
      {today} &nbsp;|&nbsp; Janela: {window_label} &nbsp;|&nbsp; {len(all_videos)} vídeos analisados
    </p>
    {top_section}
    {channels_html}
    <p style="color:#555;font-size:11px;margin-top:32px;text-align:center;line-height:1.5;">
      Score = views + (likes × 10) + (comentários × 20)<br>
      Relatório gerado automaticamente • Top 5 canais de cripto do mundo
    </p>
  </div>
</body></html>"""


# ---------- Envio ----------
def send_email(html_content):
    """Envia via API HTTP do Resend diretamente — mais simples de debugar que SDK."""
    if not RESEND_API_KEY:
        raise RuntimeError("RESEND_API_KEY não configurada.")

    print(f"📤 Enviando email...")
    print(f"   from: {SENDER_EMAIL}")
    print(f"   to:   {RECIPIENT_EMAIL}")

    r = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type":  "application/json",
        },
        json={
            "from":    SENDER_EMAIL,
            "to":      [RECIPIENT_EMAIL],
            "subject": f"🚀 Top Cripto YouTube — {datetime.now().strftime('%d/%m/%Y')}",
            "html":    html_content,
        },
        timeout=30,
    )

    print(f"   HTTP {r.status_code}")
    print(f"   Body: {r.text[:500]}")

    if r.status_code >= 400:
        raise RuntimeError(f"Resend falhou ({r.status_code}): {r.text[:300]}")

    print(f"✅ Email enviado para {RECIPIENT_EMAIL}.")


# ---------- Peak hour gate ----------
def read_peak_hour():
    """Lê a hora de pico (UTC) do arquivo. Default: 16 (16:00 UTC = 13:00 BRT)."""
    try:
        return int(PEAK_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        return 16


def should_run_now():
    """Decide se é hora de rodar baseado em RUN_MODE e peak_hour."""
    if RUN_MODE == "force":
        print("🚀 Modo force — rodando independente da hora.")
        return True

    peak_hour = read_peak_hour()
    current_hour = datetime.now(timezone.utc).hour
    if current_hour == peak_hour:
        print(f"⏰ Hora de pico ({peak_hour:02d}:00 UTC) — rodando relatório.")
        return True
    print(f"⏭️  Não é hora de pico (atual: {current_hour:02d}:00 UTC, pico: {peak_hour:02d}:00 UTC). Saindo.")
    return False


# ---------- Main ----------
def main():
    if not YOUTUBE_API_KEY:
        print("❌ YOUTUBE_API_KEY não configurada.", file=sys.stderr)
        sys.exit(1)

    if not should_run_now():
        sys.exit(0)

    channels_data = []
    total_videos_24h = 0

    for handle in TOP_CRYPTO_HANDLES:
        print(f"🔍 Resolvendo {handle}...")
        ch = resolve_channel_by_handle(handle)
        if not ch:
            print(f"   ⚠️  Canal {handle} não encontrado.")
            continue

        uploads = get_recent_uploads(ch["uploads_pid"], max_results=10)
        if not uploads:
            print(f"   ⚠️  Sem uploads recentes.")
            continue

        # Tenta janela de 24h primeiro
        recent = filter_by_window(uploads, DAILY_WINDOW_HOURS)
        window_used = f"últimas {DAILY_WINDOW_HOURS}h"

        # Se nenhum canal teve vídeo nas últimas 24h, deixamos os mais recentes do canal
        if not recent:
            recent = uploads[:MAX_VIDEOS_PER_CHANNEL]
            window_used = "vídeos mais recentes"
        else:
            recent = recent[:MAX_VIDEOS_PER_CHANNEL]
            total_videos_24h += len(recent)

        # Stats em batch
        ids = [v["video_id"] for v in recent]
        details = get_videos_details(ids)

        videos = []
        for v in recent:
            d = details.get(v["video_id"], {})
            videos.append({
                "video_id":   v["video_id"],
                "title":      v["title"],
                "published":  v["published"],
                "views":      d.get("views",    0),
                "likes":      d.get("likes",    0),
                "comments":   d.get("comments", 0),
                "engagement": engagement_score(d.get("views", 0), d.get("likes", 0), d.get("comments", 0)),
                "summary":    summarize_description(d.get("description", "")),
            })

        ch["videos"] = videos
        channels_data.append(ch)
        print(f"   ✓ {ch['name']}: {len(videos)} vídeo(s) ({window_used})")

    if not channels_data:
        print("❌ Nenhum dado coletado.", file=sys.stderr)
        sys.exit(1)

    window_label = f"Últimas {DAILY_WINDOW_HOURS}h" if total_videos_24h > 0 else f"Últimos vídeos publicados"
    html_content = build_html_report(channels_data, window_label)
    send_email(html_content)


if __name__ == "__main__":
    main()

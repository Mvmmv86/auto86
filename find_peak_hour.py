"""
Análise semanal: descobre o horário de pico de engajamento dos top canais de cripto.

Roda todo domingo:
1. Coleta últimos ~30 vídeos de cada um dos 5 canais (≈150 vídeos)
2. Agrupa engajamento por hora de publicação (UTC)
3. Define peak_hour = hora_publicação_top + 2h
4. Atualiza peak_hour.txt
5. Envia email de RESUMO SEMANAL com:
   - Top 10 vídeos da semana
   - Distribuição de engajamento por hora
   - Novo horário de pico
   - Quando o relatório diário vai rodar
"""

import html
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import resend

from crypto_daily_report import (
    TOP_CRYPTO_HANDLES,
    resolve_channel_by_handle,
    get_recent_uploads,
    get_videos_details,
    engagement_score,
    summarize_description,
    fmt,
    esc,
    YOUTUBE_API_KEY,
    RESEND_API_KEY,
    SENDER_EMAIL,
    RECIPIENT_EMAIL,
)

PEAK_FILE = Path(__file__).parent / "peak_hour.txt"
PUBLISH_BUFFER_HOURS = 2
MIN_VIDEOS_PER_HOUR  = 2


def collect_data():
    """Retorna (hour_engagements, all_videos)."""
    hour_engagements = defaultdict(list)
    all_videos = []

    for handle in TOP_CRYPTO_HANDLES:
        print(f"🔍 Analisando {handle}...")
        ch = resolve_channel_by_handle(handle)
        if not ch:
            print(f"   ⚠️  Não resolveu.")
            continue

        uploads = get_recent_uploads(ch["uploads_pid"], max_results=30)
        if not uploads:
            continue

        ids = [v["video_id"] for v in uploads]
        details = get_videos_details(ids)

        for v in uploads:
            d = details.get(v["video_id"])
            if not d:
                continue
            try:
                pub = datetime.fromisoformat(v["published"].replace("Z", "+00:00"))
            except (ValueError, KeyError, AttributeError):
                continue

            score = engagement_score(d["views"], d["likes"], d["comments"])
            hour_engagements[pub.hour].append(score)
            all_videos.append({
                "channel":     ch["name"],
                "video_id":    v["video_id"],
                "title":       v["title"],
                "published":   v["published"],
                "hour_utc":    pub.hour,
                "views":       d["views"],
                "likes":       d["likes"],
                "comments":    d["comments"],
                "engagement":  score,
                "summary":     summarize_description(d.get("description", ""), max_chars=200),
            })

        print(f"   ✓ {ch['name']}: {len(uploads)} vídeos")

    return hour_engagements, all_videos


def compute_peak_hour(hour_engagements):
    hour_avg = {h: sum(s)/len(s) for h, s in hour_engagements.items() if len(s) >= MIN_VIDEOS_PER_HOUR}
    if not hour_avg:
        hour_avg = {h: sum(s)/len(s) for h, s in hour_engagements.items()}

    publish_peak = max(hour_avg, key=hour_avg.get)
    report_hour  = (publish_peak + PUBLISH_BUFFER_HOURS) % 24
    return publish_peak, report_hour, hour_avg


def utc_to_brt(h):
    return (h - 3) % 24


def build_summary_email(all_videos, hour_avg, hour_engagements, publish_peak, report_hour):
    today = datetime.now().strftime("%d/%m/%Y")
    top10 = sorted(all_videos, key=lambda v: v["engagement"], reverse=True)[:10]

    # Top 10 vídeos da semana
    top_rows = ""
    for i, v in enumerate(top10, 1):
        title_short = esc(v['title'][:80] + ('...' if len(v['title']) > 80 else ''))
        medal = ["🥇", "🥈", "🥉"][i-1] if i <= 3 else f"#{i}"
        top_rows += f"""
        <tr style="border-bottom:1px solid #2a2a4a;">
          <td style="padding:10px 8px;text-align:center;font-weight:bold;color:#f0b429;">{medal}</td>
          <td style="padding:10px 8px;">
            <a href="https://youtu.be/{esc(v['video_id'])}" style="color:#60a5fa;text-decoration:none;font-weight:bold;">
              {title_short}
            </a>
            <div style="font-size:11px;color:#888;margin-top:3px;">{esc(v['channel'])} · {esc(v['published'][:10])} · {v['hour_utc']:02d}:00 UTC</div>
            <div style="font-size:12px;color:#bbb;margin-top:5px;line-height:1.4;">{esc(v['summary'])}</div>
          </td>
          <td style="padding:10px 8px;text-align:center;color:#34d399;white-space:nowrap;">{fmt(v['views'])}</td>
          <td style="padding:10px 8px;text-align:center;color:#f0b429;white-space:nowrap;font-weight:bold;">{fmt(v['engagement'])}</td>
        </tr>
        """

    # Heatmap de horas — barras horizontais
    max_avg = max(hour_avg.values()) if hour_avg else 1
    hour_rows = ""
    for h in range(24):
        avg = hour_avg.get(h, 0)
        n = len(hour_engagements.get(h, []))
        bar_width = int((avg / max_avg) * 100) if max_avg else 0
        is_peak = h == publish_peak
        is_report = h == report_hour
        marker = ""
        if is_peak:
            marker = ' <span style="color:#f0b429;font-weight:bold;">← pico publicação</span>'
        if is_report:
            marker += ' <span style="color:#34d399;font-weight:bold;">← envio do relatório</span>'
        bar_color = "#f0b429" if is_peak else ("#34d399" if is_report else "#60a5fa")
        hour_rows += f"""
        <tr>
          <td style="padding:4px 8px;color:#aaa;font-family:monospace;font-size:12px;white-space:nowrap;">
            {h:02d}:00 UTC ({utc_to_brt(h):02d}h BRT)
          </td>
          <td style="padding:4px 8px;width:100%;">
            <div style="background:{bar_color};height:14px;width:{bar_width}%;border-radius:3px;"></div>
          </td>
          <td style="padding:4px 8px;color:#888;font-size:11px;white-space:nowrap;">{n} vídeo(s){marker}</td>
        </tr>
        """

    return f"""<!DOCTYPE html>
<html><body style="background:#0f0f23;color:#e0e0e0;font-family:-apple-system,Arial,sans-serif;padding:24px;margin:0;">
  <div style="max-width:820px;margin:0 auto;">
    <h1 style="color:#f0b429;border-bottom:2px solid #f0b429;padding-bottom:10px;margin:0 0 8px;">
      📊 Análise Semanal — Top Cripto YouTube
    </h1>
    <p style="color:#aaa;margin:0 0 24px;font-size:13px;">
      {today} · {len(all_videos)} vídeos analisados de {len(TOP_CRYPTO_HANDLES)} canais
    </p>

    <div style="background:#1f1200;border:2px solid #f0b429;border-radius:8px;padding:18px;margin-bottom:24px;">
      <h2 style="color:#f0b429;margin:0 0 12px;font-size:18px;">⏰ Novo horário de envio do relatório diário</h2>
      <p style="margin:6px 0;font-size:22px;color:#fff;font-weight:bold;">
        {report_hour:02d}:00 UTC ({utc_to_brt(report_hour):02d}:00 BRT)
      </p>
      <p style="margin:6px 0;color:#ddd;font-size:13px;line-height:1.6;">
        Pico de publicação dos canais: <b style="color:#f0b429;">{publish_peak:02d}:00 UTC ({utc_to_brt(publish_peak):02d}:00 BRT)</b><br>
        Buffer aplicado: +{PUBLISH_BUFFER_HOURS}h (pra garantir que os vídeos já estão no ar)<br>
        A partir de agora você vai receber o relatório diário sempre nesse horário.
      </p>
    </div>

    <h2 style="color:#f0b429;margin:24px 0 12px;font-size:17px;">🏆 Top 10 vídeos da semana</h2>
    <div style="background:#16213e;border-radius:8px;overflow:hidden;">
      <table style="width:100%;border-collapse:collapse;font-size:13px;">
        <tr style="background:#0f1830;color:#999;font-size:11px;text-transform:uppercase;">
          <th style="padding:8px;text-align:center;">#</th>
          <th style="padding:8px;text-align:left;">Vídeo</th>
          <th style="padding:8px;">Views</th>
          <th style="padding:8px;">Score</th>
        </tr>
        {top_rows}
      </table>
    </div>

    <h2 style="color:#f0b429;margin:32px 0 12px;font-size:17px;">📈 Engajamento médio por hora (UTC)</h2>
    <div style="background:#16213e;border-radius:8px;padding:12px;">
      <table style="width:100%;border-collapse:collapse;">
        {hour_rows}
      </table>
    </div>

    <p style="color:#555;font-size:11px;margin-top:32px;text-align:center;line-height:1.5;">
      Score = views + (likes × 10) + (comentários × 20)<br>
      Análise rodada automaticamente todo domingo · próxima atualização em 7 dias
    </p>
  </div>
</body></html>"""


def send_summary(html_content):
    if not RESEND_API_KEY:
        print("⚠️  RESEND_API_KEY ausente — pulando envio.", file=sys.stderr)
        return
    resend.api_key = RESEND_API_KEY
    resp = resend.Emails.send({
        "from":    SENDER_EMAIL,
        "to":      [RECIPIENT_EMAIL],
        "subject": f"📊 Análise Semanal Cripto — {datetime.now().strftime('%d/%m/%Y')}",
        "html":    html_content,
    })
    print(f"✅ Email semanal enviado. ID: {resp.get('id')}")


def main():
    if not YOUTUBE_API_KEY:
        print("❌ YOUTUBE_API_KEY não configurada.", file=sys.stderr)
        sys.exit(1)

    hour_engagements, all_videos = collect_data()
    if not hour_engagements:
        print("❌ Sem dados pra analisar.", file=sys.stderr)
        sys.exit(1)

    publish_peak, report_hour, hour_avg = compute_peak_hour(hour_engagements)

    # Logs
    print(f"\n🎯 Pico de publicação:  {publish_peak:02d}:00 UTC ({utc_to_brt(publish_peak):02d}:00 BRT)")
    print(f"⏰ Horário do relatório: {report_hour:02d}:00 UTC ({utc_to_brt(report_hour):02d}:00 BRT)")
    print(f"📦 Total: {len(all_videos)} vídeos\n")

    # Atualiza arquivo
    PEAK_FILE.write_text(f"{report_hour}\n")
    print(f"✅ {PEAK_FILE} → {report_hour}")

    # Email semanal
    html_content = build_summary_email(all_videos, hour_avg, hour_engagements, publish_peak, report_hour)
    send_summary(html_content)


if __name__ == "__main__":
    main()

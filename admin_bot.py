import asyncio
import logging
from typing import Any

from telethon import Button, TelegramClient, events

from config import load_config
from app.destination_manager import (
    add_destination,
    list_destinations,
    remove_destination,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("admin_bot")


def _get_value(obj: Any, *names: str, default: Any = None) -> Any:
    """Read a value from either an object attribute or dictionary key."""
    current = obj

    for name in names:
        if isinstance(current, dict):
            current = current.get(name, default)
        else:
            current = getattr(current, name, default)

        if current is default:
            break

    return current


def _load_bot_settings() -> tuple[int, str, str, set[int]]:
    config = load_config()

    telegram = _get_value(config, "telegram", default=config)

    api_id = _get_value(telegram, "api_id")
    api_hash = _get_value(telegram, "api_hash")
    bot_token = _get_value(telegram, "bot_token")
    admin_ids = _get_value(telegram, "admin_ids", default=[])

    if not api_id:
        raise ValueError("telegram.api_id tidak dijumpai dalam config.")
    if not api_hash:
        raise ValueError("telegram.api_hash tidak dijumpai dalam config.")
    if not bot_token:
        raise ValueError("telegram.bot_token tidak dijumpai dalam config.")

    try:
        parsed_admin_ids = {int(admin_id) for admin_id in admin_ids}
    except (TypeError, ValueError) as exc:
        raise ValueError("telegram.admin_ids mesti mengandungi Telegram user ID.") from exc

    if not parsed_admin_ids:
        raise ValueError("telegram.admin_ids masih kosong.")

    return int(api_id), str(api_hash), str(bot_token), parsed_admin_ids


API_ID, API_HASH, BOT_TOKEN, ADMIN_IDS = _load_bot_settings()

client = TelegramClient("destination_admin_bot", API_ID, API_HASH)


def is_admin(user_id: int | None) -> bool:
    return user_id is not None and int(user_id) in ADMIN_IDS


async def reject_non_admin(event: events.NewMessage.Event) -> bool:
    if is_admin(event.sender_id):
        return False

    await event.respond("⛔ Anda tidak dibenarkan menggunakan bot ini.")
    return True


def main_menu() -> list[list[Button]]:
    return [
        [Button.text("➕ Tambah destination", resize=True)],
        [Button.text("📋 Senarai destination", resize=True)],
        [Button.text("➖ Buang destination", resize=True)],
        [Button.text("❌ Batal", resize=True)],
    ]


def normalize_destination(value: str) -> str:
    value = value.strip()

    if value.startswith("https://t.me/"):
        value = value.removeprefix("https://t.me/").strip("/")

    if value.startswith("t.me/"):
        value = value.removeprefix("t.me/").strip("/")

    if value and not value.startswith(("@", "-100")):
        value = f"@{value}"

    return value


async def show_menu(event: events.NewMessage.Event, message: str = "Pilih tindakan:") -> None:
    await event.respond(message, buttons=main_menu())


@client.on(events.NewMessage(pattern=r"^/(start|menu)$"))
async def start_handler(event: events.NewMessage.Event) -> None:
    if await reject_non_admin(event):
        return

    await show_menu(event, "🛠 **Destination Manager**\n\nPilih tindakan:")


@client.on(events.NewMessage(pattern=r"^/listdestinations$"))
async def list_command_handler(event: events.NewMessage.Event) -> None:
    if await reject_non_admin(event):
        return

    await send_destination_list(event)


@client.on(events.NewMessage(pattern=r"^/adddestination(?:\s+(.+))?$"))
async def add_command_handler(event: events.NewMessage.Event) -> None:
    if await reject_non_admin(event):
        return

    match = event.pattern_match
    destination = match.group(1).strip() if match and match.group(1) else ""

    if destination:
        await add_destination_and_reply(event, destination)
        return

    async with client.conversation(event.chat_id, timeout=120) as conversation:
        await conversation.send_message(
            "Hantar username atau chat ID destination.\n\n"
            "Contoh: `@channel_baru` atau `-1001234567890`\n"
            "Taip `/cancel` untuk batal."
        )

        reply = await conversation.get_response()
        value = reply.raw_text.strip()

        if value.lower() == "/cancel":
            await show_menu(event, "Dibatalkan.")
            return

        await add_destination_and_reply(event, value)


@client.on(events.NewMessage(pattern=r"^/removedestination(?:\s+(.+))?$"))
async def remove_command_handler(event: events.NewMessage.Event) -> None:
    if await reject_non_admin(event):
        return

    match = event.pattern_match
    destination = match.group(1).strip() if match and match.group(1) else ""

    if destination:
        await remove_destination_and_reply(event, destination)
        return

    destinations = list_destinations()

    if not destinations:
        await show_menu(event, "Belum ada destination untuk dibuang.")
        return

    buttons = [
        [Button.inline(f"❌ {destination}", data=f"remove:{destination}")]
        for destination in destinations
    ]
    buttons.append([Button.inline("Batal", data="cancel_remove")])

    await event.respond("Pilih destination yang hendak dibuang:", buttons=buttons)


@client.on(events.NewMessage(pattern=r"^➕ Tambah destination$"))
async def add_button_handler(event: events.NewMessage.Event) -> None:
    if await reject_non_admin(event):
        return

    async with client.conversation(event.chat_id, timeout=120) as conversation:
        await conversation.send_message(
            "Hantar username atau chat ID destination.\n\n"
            "Contoh: `@channel_baru` atau `-1001234567890`\n"
            "Taip `/cancel` untuk batal."
        )

        reply = await conversation.get_response()
        value = reply.raw_text.strip()

        if value.lower() == "/cancel":
            await show_menu(event, "Dibatalkan.")
            return

        await add_destination_and_reply(event, value)


@client.on(events.NewMessage(pattern=r"^📋 Senarai destination$"))
async def list_button_handler(event: events.NewMessage.Event) -> None:
    if await reject_non_admin(event):
        return

    await send_destination_list(event)


@client.on(events.NewMessage(pattern=r"^➖ Buang destination$"))
async def remove_button_handler(event: events.NewMessage.Event) -> None:
    if await reject_non_admin(event):
        return

    destinations = list_destinations()

    if not destinations:
        await show_menu(event, "Belum ada destination untuk dibuang.")
        return

    buttons = [
        [Button.inline(f"❌ {destination}", data=f"remove:{destination}")]
        for destination in destinations
    ]
    buttons.append([Button.inline("Batal", data="cancel_remove")])

    await event.respond("Pilih destination yang hendak dibuang:", buttons=buttons)


@client.on(events.NewMessage(pattern=r"^❌ Batal$"))
async def cancel_button_handler(event: events.NewMessage.Event) -> None:
    if await reject_non_admin(event):
        return

    await show_menu(event, "Tiada perubahan dibuat.")


@client.on(events.CallbackQuery(pattern=rb"^remove:(.+)$"))
async def remove_callback_handler(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Tidak dibenarkan.", alert=True)
        return

    destination = event.pattern_match.group(1).decode("utf-8")

    try:
        removed = remove_destination(destination)
    except Exception:
        logger.exception("Gagal membuang destination")
        await event.answer("Gagal membuang destination.", alert=True)
        return

    if removed:
        await event.edit(f"✅ Destination dibuang:\n`{destination}`")
    else:
        await event.edit(f"⚠️ Destination tidak dijumpai:\n`{destination}`")


@client.on(events.CallbackQuery(data=b"cancel_remove"))
async def cancel_remove_callback(event: events.CallbackQuery.Event) -> None:
    if not is_admin(event.sender_id):
        await event.answer("Tidak dibenarkan.", alert=True)
        return

    await event.edit("Dibatalkan. Tiada perubahan dibuat.")


async def add_destination_and_reply(
    event: events.NewMessage.Event,
    raw_destination: str,
) -> None:
    destination = normalize_destination(raw_destination)

    if not destination:
        await show_menu(event, "Destination tidak sah.")
        return

    try:
        added = add_destination(destination)
    except Exception:
        logger.exception("Gagal menambah destination")
        await show_menu(event, "❌ Gagal menambah destination. Semak log server.")
        return

    if added:
        await show_menu(event, f"✅ Destination ditambah:\n`{destination}`")
    else:
        await show_menu(event, f"⚠️ Destination sudah wujud:\n`{destination}`")


async def remove_destination_and_reply(
    event: events.NewMessage.Event,
    raw_destination: str,
) -> None:
    destination = normalize_destination(raw_destination)

    try:
        removed = remove_destination(destination)
    except Exception:
        logger.exception("Gagal membuang destination")
        await show_menu(event, "❌ Gagal membuang destination. Semak log server.")
        return

    if removed:
        await show_menu(event, f"✅ Destination dibuang:\n`{destination}`")
    else:
        await show_menu(event, f"⚠️ Destination tidak dijumpai:\n`{destination}`")


async def send_destination_list(event: events.NewMessage.Event) -> None:
    try:
        destinations = list_destinations()
    except Exception:
        logger.exception("Gagal membaca destination")
        await show_menu(event, "❌ Gagal membaca senarai destination.")
        return

    if not destinations:
        await show_menu(event, "📭 Belum ada destination.")
        return

    lines = ["📋 **Senarai destination:**", ""]

    for index, destination in enumerate(destinations, start=1):
        lines.append(f"{index}. `{destination}`")

    await show_menu(event, "\n".join(lines))


async def main() -> None:
    logger.info("Memulakan destination admin bot...")
    await client.start(bot_token=BOT_TOKEN)
    me = await client.get_me()
    logger.info("Bot aktif sebagai @%s", me.username)
    await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot dihentikan.")

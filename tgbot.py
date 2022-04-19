import logging
from postfromdb import MemesForHoursDB, sendMemesDB, RecentlyMemes
from datetime import datetime
import updData
import importlib

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext,CallbackDataCache, dispatcher
import telegram
#--------------------------
import json
import requests

from config import tg_access_token
#-----------------------

#telegram_bot_get_me = bot.get_me()
bot = telegram.Bot(token = tg_access_token)
try:
    updates = bot.get_updates()
except Exception as _ex:
    print('[INFO] while updating the bot has been gotten an error:')




logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)


def start(update: Update, context:CallbackContext) -> None:
    """Sends a message with three inline buttons attached."""
    keyboard = [
        [
            InlineKeyboardButton('recent_memes', callback_data='1'),
            InlineKeyboardButton('3h', callback_data='3'),
            InlineKeyboardButton('6h', callback_data='6'),
            InlineKeyboardButton('12h', callback_data='12'),
            InlineKeyboardButton('24h', callback_data='24')
        ],
        [InlineKeyboardButton("Upd DB", callback_data='Upd_Db')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text('choose time slot:', reply_markup=reply_markup)


def buttom(update: Update, context:CallbackContext) -> None:
    """Parses the CallbackQuery and updates the message text."""
    query = update.callback_query
    

    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery

    query.answer()
    if query.data != 'Upd_Db':


        if len(MemesForHoursDB(int(query.data))) > 0 and query.data != "1":
            sendMemesDB(update.effective_chat.id, MemesForHoursDB(int(query.data)))
            print(f"Sending memes for {query.data}")
        elif query.data == '1':
            sendMemesDB(update.effective_chat.id, RecentlyMemes())
            print("Sending recent memes")
        else:
            query.edit_message_text(text="Sorry, we don't have memes for this period. Choose other option or try later.")


    elif query.data == 'Upd_Db':
        importlib.reload(updData)
        
        query.edit_message_text(text="Data's updating, repeat your requests in a few minutes")
    else:
        query.edit_message_text(text=f'Fuck2: {query.data}, {update.effective_chat.id}')
    


def help_command(update: Update, context: CallbackContext) -> None:
    """Displays info on how to use the bot."""
    update.message.reply_text('Use /start to test this bot.')# reply_photo




def main()->None:
    """Run the bot"""
    updater = Updater(tg_access_token)
    updater.dispatcher.add_handler(CommandHandler('start', start))
    updater.dispatcher.add_handler(CallbackQueryHandler(buttom))
    updater.dispatcher.add_handler(CommandHandler('help', help_command))

    # Start the Bot
    updater.start_polling()

    # Run the bot until the user presses Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT
    updater.idle()


if __name__ == '__main__':
    main()

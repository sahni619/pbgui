import asyncio
import re
import psutil
import subprocess
import sys
import os
from pathlib import Path, PurePath
from time import sleep
from io import TextIOWrapper
from datetime import datetime
import platform
import traceback
import json
from pbgui_func import PBGDIR
from telegram import Bot, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from PBRemote import PBRemote
import re
from pbgui_purefunc import load_ini, save_ini

class PBMon():
    def __init__(self):
        self.piddir = Path(f'{PBGDIR}/data/pid')
        if not self.piddir.exists():
            self.piddir.mkdir(parents=True)
        self.pidfile = Path(f'{self.piddir}/pbmon.pid')
        self.my_pid = None
        self.offline_error = []
        self.system_error = []
        self.instance_error = []
        self.pbremote = PBRemote()
        self._telegram_token = ""
        self._telegram_chat_id = ""
        self.bot_application = None
        
    @property
    def telegram_token(self):
        if not self._telegram_token:
            self._telegram_token = load_ini("main", "telegram_token")
        return self._telegram_token
    @telegram_token.setter
    def telegram_token(self, new_telegram_token):
        if self._telegram_token != new_telegram_token:
            self._telegram_token = new_telegram_token
            save_ini("main", "telegram_token", new_telegram_token)

    @property
    def telegram_chat_id(self):
        if not self._telegram_chat_id:
            self._telegram_chat_id = load_ini("main", "telegram_chat_id")
        return self._telegram_chat_id
    @telegram_chat_id.setter
    def telegram_chat_id(self, new_telegram_chat_id):
        if self._telegram_chat_id != new_telegram_chat_id:
            self._telegram_chat_id = new_telegram_chat_id
            save_ini("main", "telegram_chat_id", new_telegram_chat_id)
    
    def run(self):
        if not self.is_running():
            cmd = [sys.executable, '-u', PurePath(f'{PBGDIR}/PBMon.py')]
            if platform.system() == "Windows":
                creationflags = subprocess.DETACHED_PROCESS
                creationflags |= subprocess.CREATE_NO_WINDOW
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=PBGDIR, text=True, creationflags=creationflags)
            else:
                subprocess.Popen(cmd, stdout=None, stderr=None, cwd=PBGDIR, text=True, start_new_session=True)
            count = 0
            while True:
                if count > 5:
                    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: Can not start PBMon')
                sleep(1)
                if self.is_running():
                    break
                count += 1

    def stop(self):
        if self.is_running():
            print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Stop: PBMon')
            psutil.Process(self.my_pid).kill()

    def restart(self):
        if self.is_running():
            self.stop()
            self.run()

    def is_running(self):
        self.load_pid()
        try:
            if self.my_pid and psutil.pid_exists(self.my_pid) and any(sub.lower().endswith("pbmon.py") for sub in psutil.Process(self.my_pid).cmdline()):
                return True
        except psutil.NoSuchProcess:
            pass
        return False

    def load_pid(self):
        if self.pidfile.exists():
            with open(self.pidfile) as f:
                pid = f.read()
                self.my_pid = int(pid) if pid.isnumeric() else None

    def save_pid(self):
        self.my_pid = os.getpid()
        with open(self.pidfile, 'w') as f:
            f.write(str(self.my_pid))
    
    async def send_telegram_message(self, message):
        bot = Bot(token=self.telegram_token)
        async with bot:
            await bot.send_message(chat_id=self.telegram_chat_id, text=message, parse_mode='Markdown')

    async def telegram_bot(self):
        if not self.telegram_token:
            return
        self.bot_application = ApplicationBuilder().token(self.telegram_token).build()
        self.bot_application.add_handler(CommandHandler("panic", self.cmd_panic))
        self.bot_application.add_handler(CommandHandler("normal", self.cmd_normal))
        self.bot_application.add_handler(CommandHandler("graceful_stop", self.cmd_graceful))
        self.bot_application.add_handler(CommandHandler("help", self.cmd_help))
        await self.bot_application.run_polling()

    def set_instance_mode(self, instance_name: str, long_mode: str, short_mode: str):
        p = Path(f"{PBGDIR}/data/instances/{instance_name}/instance.cfg")
        if not p.exists():
            return False
        try:
            with open(p, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            cfg["_long_mode"] = long_mode
            cfg["_short_mode"] = short_mode
            with open(p, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=4)
            self.pbremote.local_run.activate(instance_name, False)
            return True
        except Exception as e:
            print(f"Failed to update instance {instance_name}: {e}")
            traceback.print_exc()
        return False

    async def cmd_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE, long_mode: str, short_mode: str):
        args = context.args
        if len(args) != 3:
            await update.message.reply_text("Usage: /<cmd> <user> <symbol> <market>")
            return
        instance = f"{args[0]}_{args[1]}_{args[2]}"
        if self.set_instance_mode(instance, long_mode, short_mode):
            await update.message.reply_text(f"{instance} set to {long_mode}")
        else:
            await update.message.reply_text(f"Instance {instance} not found")

    async def cmd_panic(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.cmd_mode(update, context, "panic", "panic")

    async def cmd_graceful(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.cmd_mode(update, context, "graceful_stop", "graceful_stop")

    async def cmd_normal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        await self.cmd_mode(update, context, "normal", "normal")

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        msg = (
            "Commands:\n"
            "/panic <user> <symbol> <market> - set panic mode\n"
            "/graceful_stop <user> <symbol> <market> - set graceful_stop mode\n"
            "/normal <user> <symbol> <market> - set normal mode"
        )
        await update.message.reply_text(msg)

    async def has_errors(self):
        self.pbremote.update_remote_servers()
        errors = self.pbremote.has_error()
        if errors:
            msg = ""
            for error in errors:
                if error["name"] == "offline":
                    if error["server"] not in self.offline_error:
                        self.offline_error.append(error["server"])
                        msg = msg + f'Server: *{error["server"]}* is offline\n'
                elif error["name"] == "system":
                    if error["server"] not in self.system_error:
                        self.system_error.append(error["server"])
                        msg = msg + f'Server: {error["server"]} Instance: {error["name"]} Mem: {error["mem"]} CPU: {error["cpu"]} Swap: {error["swap"]} Disk: {error["disk"]}\n'
                else:
                    if error["name"] not in self.instance_error:
                        self.instance_error.append(error["name"])
                        msg = msg + f'Server: {error["server"]} Instance: {error["name"]} Mem: {error["mem"]} CPU: {error["cpu"]} Error: {error["error"]} Traceback: {error["traceback"]}\n'
            # remove errors that are no longer present
            self.offline_error = [error for error in self.offline_error if error in [error["server"] for error in errors if error["name"] == "offline"]]
            self.system_error = [error for error in self.system_error if error in [error["server"] for error in errors if error["name"] == "system"]]
            self.instance_error = [error for error in self.instance_error if error in [error["name"] for error in errors if error["name"] not in ["offline", "system"]]]
            msg = re.sub(r':blue\[(.*?)\]', r'*\1*', msg)
            msg = re.sub(r':red\[(.*?)\]', r'*\1*', msg)
            msg = re.sub(r':green\[(.*?)\]', r'\1', msg)
            msg = re.sub(r':orange\[(.*?)\]', r'\1', msg)
            if msg:
                print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Send Message:\n{msg}')
                await self.send_telegram_message(msg)

    async def monitor_loop(self, logfile: Path):
        while True:
            try:
                if logfile.exists() and logfile.stat().st_size >= 10485760:
                    logfile.replace(f"{str(logfile)}.old")
                    sys.stdout = TextIOWrapper(open(logfile, "ab", 0), write_through=True)
                    sys.stderr = TextIOWrapper(open(logfile, "ab", 0), write_through=True)
                if self.telegram_token and self.telegram_chat_id:
                    await self.has_errors()
                await asyncio.sleep(60)
            except Exception as e:
                print(f'Something went wrong, but continue {e}')
                traceback.print_exc()

    async def run_async(self, logfile: Path):
        tasks = [asyncio.create_task(self.monitor_loop(logfile))]
        if self.telegram_token and self.telegram_chat_id:
            tasks.append(asyncio.create_task(self.telegram_bot()))
        await asyncio.gather(*tasks)
   

def main():
    dest = Path(f'{PBGDIR}/data/logs')
    if not dest.exists():
        dest.mkdir(parents=True)
    logfile = Path(f'{str(dest)}/PBMon.log')
    sys.stdout = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    sys.stderr = TextIOWrapper(open(logfile,"ab",0), write_through=True)
    print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Start: PBMon')
    pbmon = PBMon()
    if pbmon.is_running():
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        print(f'{datetime.now().isoformat(sep=" ", timespec="seconds")} Error: PBMon already started')
        exit(1)
    pbmon.save_pid()
    try:
        asyncio.run(pbmon.run_async(logfile))
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    main()

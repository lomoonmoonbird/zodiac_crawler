# --*-- coding:utf-8 --*--

from settings.debug_settings import DebugSettings
from settings.settings_base import SettingsBase

current_settings = SettingsBase()


def change_setting(new_setting):
    global current_settings
    current_settings = new_setting


def get_current_setting():
    global current_settings
    return current_settings

available_environments = {
        'debug': DebugSettings()
    }

change_setting(available_environments['debug'])
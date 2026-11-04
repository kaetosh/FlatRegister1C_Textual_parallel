# -*- coding: utf-8 -*-
"""UI-тесты: структура экранов и модалок поверх run_test()."""

from __future__ import annotations

import asyncio

from textual.widgets import Select

from ui.app import FlatRegister1CApp
from ui.screens import ContactsScreen, RegisterScreen, SettingsScreen, SettingsScreenRegister


def _run(coro):
    asyncio.run(coro)


async def _f3_opens_and_escape_closes_settings():
    app = FlatRegister1CApp()
    async with app.run_test() as pilot:
        await pilot.pause()
        assert len(app.screen_stack) == 1

        await pilot.press("f3")
        await pilot.pause()
        assert len(app.screen_stack) == 2
        assert isinstance(app.screen, SettingsScreen)

        await pilot.press("escape")
        await pilot.pause()
        assert len(app.screen_stack) == 1


async def _f1_opens_contacts_and_escape_closes():
    app = FlatRegister1CApp()
    async with app.run_test() as pilot:
        await pilot.pause()

        await pilot.press("f1")
        await pilot.pause()
        assert isinstance(app.screen, ContactsScreen)

        await pilot.press("escape")
        await pilot.pause()
        assert len(app.screen_stack) == 1


async def _select_and_continue_pushes_register_screen():
    app = FlatRegister1CApp()
    async with app.run_test() as pilot:
        await pilot.pause()

        select = app.query_one("#select_register", Select)
        button = app.query_one("#continue")

        assert button.disabled

        value = select.value
        if value is Select.BLANK:
            select.value = "card"
            await pilot.pause()
            button.disabled = False

        if not button.disabled:
            await pilot.click("#continue")
            await pilot.pause()
            assert isinstance(app.screen, RegisterScreen)


async def _cancel_in_settings_closes_without_saving():
    """generalosv: Cancel closes modal without saving config."""
    app = FlatRegister1CApp()
    async with app.run_test() as pilot:
        await pilot.pause()

        app.register = "generalosv"
        app.push_screen(SettingsScreenRegister(register_type="generalosv"))
        await pilot.pause()
        assert isinstance(app.screen, SettingsScreenRegister)

        cancel = app.screen.query_one("#button-cancel-settings-modal")
        await pilot.click(cancel)
        await pilot.pause()
        assert len(app.screen_stack) == 1


async def _escape_in_settings_closes():
    app = FlatRegister1CApp()
    async with app.run_test() as pilot:
        await pilot.pause()

        app.push_screen(SettingsScreen())
        await pilot.pause()
        assert isinstance(app.screen, SettingsScreen)

        await pilot.press("escape")
        await pilot.pause()
        assert len(app.screen_stack) == 1


def test_f3_opens_settings_and_escape_closes():
    _run(_f3_opens_and_escape_closes_settings())


def test_f1_opens_contacts_and_escape_closes():
    _run(_f1_opens_contacts_and_escape_closes())


def test_select_and_continue_pushes_register_screen():
    _run(_select_and_continue_pushes_register_screen())


def test_cancel_in_generalosv_settings_closes_without_saving():
    _run(_cancel_in_settings_closes_without_saving())


def test_escape_in_settings_closes():
    _run(_escape_in_settings_closes())

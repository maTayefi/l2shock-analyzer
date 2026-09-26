# l2shock/ui/shutdown_control.py
"""Global header Application Shutdown control (visible on every tab).

Same confirmation and the same idempotent shutdown_runtime path that the
Settings tab used before; only the placement changed.
"""

from __future__ import annotations

from nicegui import ui

from l2shock.ui.components import create_tracked_task, persistent_notify
from l2shock.ui.shutdown import shutdown_runtime
from l2shock.ui.state import get_state


def build_shutdown_header_button() -> None:
    """Create the header Shutdown button and its confirmation dialog."""
    state = get_state()

    with ui.dialog() as shutdown_dialog:
        with ui.card().classes("w-[34rem] max-w-full"):
            ui.label("Confirm Application Shutdown").classes(
                "text-lg font-bold text-red-500"
            )
            ui.label(
                "Application Shutdown blocks new operations, requests "
                "cooperative Fetch, Processing, and Analysis cancellation, "
                "waits for durable operation boundaries, cancels remaining "
                "tracked tasks, disposes the database engine, and then stops "
                "the local NiceGUI server."
            ).classes("text-sm text-gray-600")
            ui.label(
                "The browser connection will close after active work is "
                "stopped safely. You will need to run run_app.bat again to "
                "restart the application."
            ).classes("text-sm")

            with ui.row().classes("w-full justify-end gap-2 mt-4"):
                ui.button(
                    "Cancel",
                    on_click=shutdown_dialog.close,
                ).props("flat")

                confirm_button = ui.button(
                    "Stop application safely",
                    icon="power_settings_new",
                ).props("color=negative")

    shutdown_button = (
        ui.button(
            "Shutdown",
            icon="power_settings_new",
        )
        .props("color=negative unelevated dense")
        .classes("ml-3 px-3")
        .tooltip("Application Shutdown (safe, confirmed)")
    )

    def _open_shutdown_dialog() -> None:
        if state.shutdown_started:
            ui.notify(
                "Application shutdown is already in progress.",
                type="warning",
            )
            return

        shutdown_dialog.open()

    async def _confirmed_shutdown() -> None:
        if state.shutdown_started:
            return

        shutdown_button.disable()
        confirm_button.disable()
        shutdown_dialog.close()

        persistent_notify(
            "Safe application shutdown has started. Active operations are "
            "being stopped and finalized.",
            title="Application Shutdown",
            notification_type="warning",
        )

        task = create_tracked_task(
            shutdown_runtime(request_server_stop=True),
            name="l2shock-application-shutdown",
        )

        if task is None:
            shutdown_button.enable()
            confirm_button.enable()
            persistent_notify(
                "Could not create the application shutdown task.",
                title="Application Shutdown",
                notification_type="negative",
            )

    shutdown_button.on_click(_open_shutdown_dialog)
    confirm_button.on_click(_confirmed_shutdown)


__all__ = ["build_shutdown_header_button"]
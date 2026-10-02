"""Verification helpers (reference receiver + control client).

Kept import-free so ``python -m webhooks.testing.receiver`` starts without
importing the module twice; import the submodules directly:

    from webhooks.testing.receiver import create_receiver_app
    from webhooks.testing.client import ReceiverClient
"""

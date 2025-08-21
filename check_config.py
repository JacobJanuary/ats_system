from ats_system.core.config import Config

config = Config()
print(f"Stop Loss Type: {config.risk.stop_loss_type}")
print(f"Trailing Callback Rate: {config.risk.trailing_callback_rate}%")
print(f"Activation Percent: {config.risk.trailing_activation_percent}%")
print(f"Stop Loss Percent: {config.risk.stop_loss_percent}%")

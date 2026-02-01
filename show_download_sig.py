from py2rocket import download
import inspect

sig = inspect.signature(download)
print("Firma de download():")
print(f"  download{sig}")
print("\nParametros:")
for name, param in sig.parameters.items():
    annotation = (
        param.annotation if param.annotation != inspect.Parameter.empty else "Any"
    )
    default = f" = {param.default}" if param.default != inspect.Parameter.empty else ""
    print(f"  - {name}: {annotation}{default}")

from loguru import logger
import sys

custom_format = (
    "| {thread.name} | <green>{time:YYYY-MM-DD HH:mm:ss}</green> "
    "| <level>{level: <8}</level> "
    "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
    "- {message}"
)
# custom_format = (
#     "| {thread.name} | {time:YYYY-MM-DD HH:mm:ss.SSS} | {level} | {process} | "
#     "{name}:{function}:{line} | {message}"
# )
# Remove the default handler
logger.remove()

# Add a new handler with the custom format
logger.add(sys.stdout, format=custom_format)

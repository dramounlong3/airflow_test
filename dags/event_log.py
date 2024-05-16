from loguru import logger
import os
import sys
import inspect


class Event_log:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log_file_path = os.path.join(current_dir, "logs", f"config_module_{{time:YYYY-MM-DD}}.log")

    #logger.info, logger.success, logger.warning, logger.error, logger.critical已定義
    logger.level("alertinfo", no=23, color="<bold>")

    # log file
    # logger.add(
    #       log_file_path,
    #       rotation="00:00",
    #       format="{time} | {level:<10} | {message}",
    #       level="DEBUG",
    #       retention="30 days"
    # )

    # terminal
    logger.add(
        sys.stderr,
        level="TRACE",
        format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<10} | {message}"
    )

    @staticmethod
    # level: trace, debug, info, alertinfo, success, warning, error, critical
    def log_message(level, message):
        frame = inspect.currentframe().f_back
        function_name = frame.f_code.co_name
        line_number = frame.f_lineno
        class_name = frame.f_locals['self'].__class__.__name__ if 'self' in frame.f_locals else '__main__'
        file_name = os.path.basename(frame.f_code.co_filename)

        full_message = f"{file_name} | {class_name}.{function_name}:{line_number} - {message}"

        logger_function = getattr(logger, level.lower(), logger.info)
        logger_function(full_message)
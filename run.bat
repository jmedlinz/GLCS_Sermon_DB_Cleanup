@ECHO OFF

@REM :: Check for any command line parameters
@REM IF %1.==. (set "path_param=") else (set path_param=-p %1)

cls

cd c:\users\jmedlin\documents\github\glcs_sermon_db_cleanup

@REM :: Make sure pyenv is set up
@REM set PATH=%PATH%;c:\users\svc_python\.pyenv\pyenv-win\bin
@REM set PATH=%PATH%;c:\users\svc_python\.pyenv\pyenv-win\shims
@REM set PYENV=c:\users\svc_python\.pyenv\pyenv-win\
@REM set PYENV_HOME=%PYENV%
@REM set PYENV_ROOT=%PYENV%

:: Run the Python script
@REM poetry run python main.py %path_param%
poetry run python main.py

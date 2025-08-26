Attribute VB_Name = "ModuleExcelAgent"
Option Explicit

' This VBA module provides simple buttons for the health reporting agent. Each
' public Sub calls a Python function via the system shell. The macros assume
' that Python is available on the system PATH. Adjust the python command or
' provide an absolute path to python.exe if needed.
'
' Botao_AtualizarTudo:
'   Runs the end‑to‑end workflow: imports raw files, cleans data, calculates
'   pivots/rankings and writes the Excel sheets. It also updates the audit
'   sheet and logs to file.
'
' Botao_GerarGraficos:
'   Reads the existing Dados sheet and regenerates the charts on the Resumo
'   sheet. Use this after manually editing the data.
'
' Botao_GerarRankings:
'   Recomputes only the rankings sheet based on the current data.

Public Sub Botao_AtualizarTudo()
    Call RunPythonAgent("atualizar_tudo")
End Sub

Public Sub Botao_GerarGraficos()
    Call RunPythonAgent("gerar_graficos")
End Sub

Public Sub Botao_GerarRankings()
    Call RunPythonAgent("gerar_rankings")
End Sub

Private Sub RunPythonAgent(ByVal action As String)
    Dim wbPath As String
    Dim scriptPath As String
    Dim cmd As String
    ' Determine workbook directory; this assumes the workbook and the scripts
    ' folder live in the same top‑level directory.
    wbPath = ThisWorkbook.Path
    scriptPath = wbPath & Application.PathSeparator & "scripts" & Application.PathSeparator & "agent.py"
    ' Compose the shell command. Wrap paths in double quotes to handle spaces.
    cmd = "python """ & scriptPath & """ " & action
    ' Execute the command asynchronously (vbHide prevents flashing a console window)
    Shell cmd, vbNormalFocus
End Sub
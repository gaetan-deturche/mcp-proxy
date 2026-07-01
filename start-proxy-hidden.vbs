' Launch mcp-proxy.exe (sitting next to this script) in HTTP mode, no console window.
Set fso = CreateObject("Scripting.FileSystemObject")
Set sh  = CreateObject("WScript.Shell")
dir = fso.GetParentFolderName(WScript.ScriptFullName)
sh.CurrentDirectory = dir
sh.Run """" & dir & "\mcp-proxy.exe"" -http 127.0.0.1:6390", 0, False

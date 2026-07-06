Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
root = fso.GetParentFolderName(WScript.ScriptFullName)
cmd = Chr(34) & fso.BuildPath(root, "start_paper_dashboard.cmd") & Chr(34)
shell.Run cmd, 0, False

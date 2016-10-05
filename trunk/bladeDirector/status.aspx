<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="status.aspx.cs" Inherits="bladeDirector.status" %>

<!DOCTYPE html>

<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
    <title></title>
</head>
<body>
    <form id="form1" runat="server">
    <div>
        <asp:Table ID="tblBladeStatus" runat="server"></asp:Table>
    </div>
    <asp:Button ID="cmdReset" runat="server" Text="Reset all nodes to unused" OnClick="cmdReset_Click" />
    </form>
</body>
</html>

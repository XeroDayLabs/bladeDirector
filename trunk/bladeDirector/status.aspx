<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="status.aspx.cs" Inherits="bladeDirector.status" %>

<!DOCTYPE html>

<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
    <title></title>
    <style>
        body { background: purple }
        td { background-color: blue; color: white }
        td.timedout {background-color: pink }
        td.invisible  { background-color: transparent; border-width: 0px }
        th { background-color: lightgoldenrodyellow; color: black }
        table.settingGroup { width: 100%; border-style: solid; border-color: gray; background: black }
        td.tableSpacer { background: transparent }
        div.fixedSize {font-family: monospace; border-style: solid; border-color: gray; }
    </style>
    
    <script type="text/javascript" src="jquery.js"></script>
    <script type="text/javascript">

        function toggleDetail(elem, id) {
            var clickedRow = $(elem[0]).parents('tr');
            var rowToHide = clickedRow.next('tr');
            if ($(rowToHide).is(":hidden"))
                elem[0].src = "images/expanded.png";
             else
                elem[0].src = "images/collapsed.png";
            rowToHide.slideToggle();
        }


        function toggleConfigBox(elem, url) {
            var parentCell = $(elem[0]).parent('td')[0];
            var textControl = $(parentCell).children('div')[0];

            if ($(textControl).is(":hidden")) {
                elem[0].src = "images/expanded.png";
                doShow(url, textControl);
            } else {
                elem[0].src = "images/collapsed.png";
                $(textControl).slideUp();
            }
        }
        
        function doShow(url, ctrl)
        {
            $(ctrl).slideUp();

            $.ajax({
                type: "GET",
                url: url,
                control: ctrl,
                async: true,
                success: function (text) {
                    ctrl = this.control;
                    ctrl.innerText = text;
                    $(ctrl).slideDown();
                }
            });
        }

    </script>

</head>
<body>
    <form id="form1" runat="server">
    <div>
        <table style="width: 100%" class="settingGroup">
            <tr>
                <th>Node status</th>
            </tr>
            <tr style="width: 100%">
                <asp:Table ID="tblBladeStatus" runat="server"  style="border-style: solid; width: 100%"></asp:Table>
            </tr>
        </table>
    </div>

    <br/><br/>
        
    <div>
        <table class="settingGroup">
            <tr>
                <th>Log events</th>
            </tr>
            <tr style="min-height: 10px">
                <td>
                    <asp:ListBox ID="lstLog" runat="server" style="width: 100%"> </asp:ListBox>
                </td>
            </tr>
        </table>
    </div>
        
    <br/><br/>

    <div >
        <table style="margin-left: 25%; margin-right: 25%; width: 50%;" class="settingGroup">
            <th colspan="4">
                Add a new node
            </th>
            <tr>
                <td>Node IP</td>
                <td style="width: 25%" ><asp:TextBox ID="txtNewNodeIP" style="width: 100%" runat="server">172.17.129.</asp:TextBox></td>
            </tr>
            <tr>
                <td>iLo IP</td>
                <td style="width: 25%" ><asp:TextBox ID="txtNewIloIP" style="width: 100%" runat="server">172.17.2.</asp:TextBox></td>
            </tr>
            <tr>
                <td style="left: 25%">Kenerl debug port</td>
                <td style="width: 25%" ><asp:TextBox ID="txtNewPort" style="width: 100%" runat="server">5100x</asp:TextBox></td>
            </tr>
            <tr>
                <td style="width: 25%">iSCSI IP</td>
                <td style="width: 25%" ><asp:TextBox ID="txtNewISCSI" style="width: 100%" runat="server">192.168.66.</asp:TextBox></td>
            </tr>
            <th colspan="4">
                <asp:Button ID="cmdAddNode" runat="server" Text="Add node" OnClick="cmdAddNode_Click" />
            </th>
        </table>
        
    </div>

    <div>
        <asp:Button ID="cmdReset" runat="server" Text="Delete all nodes" OnClick="cmdReset_Click" />
    </div>
    </form>
</body>
</html>

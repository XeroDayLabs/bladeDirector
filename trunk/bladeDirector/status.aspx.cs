using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;

namespace bladeDirector
{
    public partial class status : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            TableRow headerRow = new TableRow();
            headerRow.Cells.Add(new TableHeaderCell() { Text = "" });
            headerRow.Cells.Add(new TableHeaderCell() { Text = "State" });
            headerRow.Cells.Add(new TableHeaderCell() { Text = "Blade IP" });
            headerRow.Cells.Add(new TableHeaderCell() { Text = "Time since last keepalive"});
            headerRow.Cells.Add(new TableHeaderCell() { Text = "Currently-selected snapshot" });
            headerRow.Cells.Add(new TableHeaderCell() { Text = "Current owner" });
            headerRow.Cells.Add(new TableHeaderCell() { Text = "Next owner" });
            headerRow.Cells.Add(new TableHeaderCell() { Text = "Actions" });

            tblBladeStatus.Rows.Add(headerRow);

            List<bladeOwnership> allBladeInfo = hostStateDB.getAllBladeInfo();

            foreach (bladeOwnership bladeInfo in allBladeInfo)
            {
                // First, assemble the always-visible status row
                TableRow newRow = new TableRow();

                newRow.Cells.Add(makeTableCell(new Button() {
                        Text = "Details", OnClientClick = "javascript:showDetail($(this), " + bladeInfo.bladeID + "); return false;" 
                }));
                newRow.Cells.Add(new TableCell() {Text = bladeInfo.state.ToString()});
                newRow.Cells.Add(new TableCell() {Text = bladeInfo.bladeIP});
                if (bladeInfo.lastKeepAlive == DateTime.MinValue)
                {
                    newRow.Cells.Add(new TableCell() {Text = "(none)"});
                }
                else
                {
                    string cssClass = "";
                    if (DateTime.Now - bladeInfo.lastKeepAlive > hostStateDB.keepAliveTimeout)
                        cssClass = "timedout";
                    TableCell cell = new TableCell
                    {
                        Text = (DateTime.Now - bladeInfo.lastKeepAlive).ToString(),
                        CssClass = cssClass
                    };
                    newRow.Cells.Add(cell);
                }
                newRow.Cells.Add(new TableCell() { Text = bladeInfo.currentSnapshot});
                newRow.Cells.Add(new TableCell() { Text = bladeInfo.currentOwner ?? "none" });
                newRow.Cells.Add(new TableCell() { Text = bladeInfo.nextOwner ?? "none" });

                Button btnRelease = new Button
                {
                    Text = "Force release",
                    CommandArgument = bladeInfo.bladeIP
                };
                btnRelease.Click += forceRelease;

                newRow.Cells.Add(makeTableCell(btnRelease));
                tblBladeStatus.Rows.Add(newRow);

                // Then populate the invisible-until-expanded details row.
                tblBladeStatus.Rows.Add(makeDetailRow(bladeInfo));
                
                
                // Finally, populate any log events.
                List<string> logEvents = hostStateDB.getLogEvents();
                foreach (string logEvent in logEvents)
                    lstLog.Items.Add(logEvent);
            }
        }

        private TableRow makeDetailRow(bladeOwnership bladeInfo)
        {
            Table detailTable = new Table();

            TableRow pxeScriptRow = new TableRow();
            pxeScriptRow.Cells.Add(new TableCell() {Text = "Current PXE script: "});
            pxeScriptRow.Cells.Add(makeTableCell(
                makeJSHyperLink("show", string.Format(@"javascript:showPXEConfig($(this), ""{0}""); return false;", bladeInfo.bladeIP)),
                makeJSHyperLink("hide", string.Format(@"javascript:hideConfig($(this), ""{0}""); return false;", bladeInfo.bladeIP)), 
                new HtmlGenericControl("div")
                ));

            detailTable.Rows.Add(pxeScriptRow);

            TableRow biosConfigRow = new TableRow();
            biosConfigRow.Cells.Add(new TableCell() { Text = "Current BIOS configuration: "});
            biosConfigRow.Cells.Add(makeTableCell(
                makeJSHyperLink("show", string.Format(@"javascript:showBIOSConfig($(this), ""{0}""); return false;", bladeInfo.bladeIP)),
                makeJSHyperLink("hide", string.Format(@"javascript:hideConfig($(this), ""{0}""); return false;", bladeInfo.bladeIP)),
                new HtmlGenericControl("div")));
            detailTable.Rows.Add(biosConfigRow);

            TableHeaderRow toRet = new TableHeaderRow();
            TableCell tc = new TableCell();
            tc.ColumnSpan = 8;
            tc.Controls.Add(detailTable);
            toRet.Cells.Add(tc);
            toRet.Style.Add("display", "none");
            return toRet;
        }

        private HyperLink makeJSHyperLink(string text, string onClick)
        {
            HyperLink hl = new HyperLink() {Text = text};
            hl.Attributes.Add("onclick", onClick);
            hl.Attributes.Add("href", "#");
            return hl;
        }

        private TableCell makeTableCell(params Control[] innerControl)
        {
            TableCell tc = new TableCell();
            foreach (Control control in innerControl)
                tc.Controls.Add(control);
            return tc;
        }

        private void forceRelease(object sender, EventArgs e)
        {
            Button clicked = (Button) sender;

            hostStateDB.releaseBlade(clicked.CommandArgument, "console", true);
        }

        protected void cmdReset_Click(object sender, EventArgs e)
        {
            hostStateDB.resetAll();
        }

        protected void cmdAddNode_Click(object sender, EventArgs e)
        {
            bladeOwnership newBlade = new bladeOwnership(txtNewNodeIP.Text, txtNewISCSI.Text, txtNewIloIP.Text, ushort.Parse(txtNewPort.Text), "-clean", null);
            hostStateDB.addNode(newBlade);
        }
    }
}
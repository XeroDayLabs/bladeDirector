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

            List<bladeSpec> allBladeInfo = hostStateDB.getAllBladeInfo();

            foreach (bladeSpec bladeInfo in allBladeInfo)
            {
                // First, assemble the always-visible status row
                TableRow newRow = new TableRow();

                newRow.Cells.Add(makeTableCell(new ImageButton() {
                        ImageUrl = "images/collapsed.png",
                        AlternateText = "Details",
                        OnClientClick = "javascript:toggleDetail($(this), " + bladeInfo.bladeID + "); return false;" 
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
                        Text = formatDateTimeForWeb((DateTime.Now - bladeInfo.lastKeepAlive)),
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

        private string formatDateTimeForWeb(TimeSpan toshow)
        {
            if (toshow > TimeSpan.FromHours(24))
                return " > 24 hours ";
            return String.Format("{0}h {1}m {2}s", toshow.Hours, toshow.Minutes, toshow.Seconds );
        }

        private TableRow makeDetailRow(bladeSpec bladeInfo)
        {
            Table detailTable = new Table();

            TableRow miscTR = new TableRow();
            miscTR.Cells.Add(makeTableCell(
                new Label() { Text = "Blade DB ID: " },
                new Label() { Text = bladeInfo.bladeID.ToString() + "<br/>", CssClass = "fixedSize"},
                new Label() { Text = "ISCSI IP: "},
                new Label() { Text = bladeInfo.iscsiIP.ToString() + "<br/>", CssClass = "fixedSize" },
                new Label() { Text = "Kernel debug port: " },
                new Label() { Text = bladeInfo.iLOPort.ToString() + "<br/>", CssClass = "fixedSize" },
                new Label() { Text = "Is currently having BIOS config deployed: " },
                new Label() { Text = bladeInfo.currentlyHavingBIOSDeployed.ToString() + "<br/>", CssClass = "fixedSize" },
                new Label() { Text = "Is currently acting as VM server: " },
                new Label() { Text = bladeInfo.currentlyBeingAVMServer.ToString() + "<br/>", CssClass = "fixedSize"}
            ));
            detailTable.Rows.Add(miscTR);

            TableRow pxeScriptRow = new TableRow();
            pxeScriptRow.Cells.Add(makeTableCell(
                makeImageButton("show", "images/collapsed.png", string.Format(@"javascript:toggleConfigBox($(this), ""getIPXEScript.aspx?hostip={0}""); return false;", bladeInfo.bladeIP)),
                new Label() { Text = "Current PXE script" },
                makeInvisibleDiv()
                ));
            detailTable.Rows.Add(pxeScriptRow);

            TableRow biosConfigRow = new TableRow();
            biosConfigRow.Cells.Add(makeTableCell(
                makeImageButton("show", "images/collapsed.png", string.Format(@"javascript:toggleConfigBox($(this), ""getBIOSConfig.ashx?hostip={0}""); return false;", bladeInfo.bladeIP)),
                new Label() { Text = "Current BIOS configuration" },
                makeInvisibleDiv()
                ));
            detailTable.Rows.Add(biosConfigRow);

            // And add rows for any VMs.
            vmSpec[] VMs = hostStateDB.getVMByVMServerIP(bladeInfo.bladeIP);
            if (VMs.Length > 0)
            {
                TableRow VMHeaderRow = new TableRow();
                VMHeaderRow.Cells.Add(new TableHeaderCell() { Text = "" });
                VMHeaderRow.Cells.Add(new TableHeaderCell() { Text = "Child VM IP" });
                VMHeaderRow.Cells.Add(new TableHeaderCell() { Text = "iSCSI IP" });
                VMHeaderRow.Cells.Add(new TableHeaderCell() { Text = "Current owner" });
                VMHeaderRow.Cells.Add(new TableHeaderCell() { Text = "Next owner" });
                VMHeaderRow.Cells.Add(new TableHeaderCell() { Text = "Current snapshot" });
                detailTable.Rows.Add(VMHeaderRow);
            }
            foreach (vmSpec vmInfo in VMs)
            {
                TableRow thisVMRow = new TableRow();

                thisVMRow.Cells.Add(makeTableCell(
                    makeImageButton("show", "images/collapsed.png", string.Format(@"javascript:toggleConfigBox($(this), ""getIPXEScript.aspx?hostip={0}""); return false;", vmInfo.VMIP)),
                    new Label() { Text = "Current PXE script" },
                    makeInvisibleDiv()
                    ));

                thisVMRow.Cells.Add(new TableCell() { Text = vmInfo.VMIP });
                thisVMRow.Cells.Add(new TableCell() { Text = vmInfo.iscsiIP });
                thisVMRow.Cells.Add(new TableCell() { Text = vmInfo.currentOwner });
                thisVMRow.Cells.Add(new TableCell() { Text = vmInfo.nextOwner });
                thisVMRow.Cells.Add(new TableCell() { Text = vmInfo.currentSnapshot });
                detailTable.Rows.Add(thisVMRow);
            }

            TableHeaderRow toRet = new TableHeaderRow();
            TableCell paddingCell = new TableCell { CssClass = "invisible" };
            toRet.Cells.Add(paddingCell);
            TableCell tc = new TableCell();
            tc.ColumnSpan = 7;
            tc.Controls.Add(detailTable);
            toRet.Cells.Add(tc);
            toRet.Style.Add("display", "none");

            return toRet;
        }

        private static HtmlGenericControl makeInvisibleDiv()
        {
            HtmlGenericControl toRet = new HtmlGenericControl("div");
            toRet.Style.Add("display", "none");
            toRet.Attributes["class"] = "fixedSize";

            return toRet;
        }

        private Control makeImageButton(string text, string imageURL, string onClick, bool isVisible = true)
        {
            ImageButton toRet = new ImageButton()
            {
                ImageUrl = imageURL,
                AlternateText = text,
                OnClientClick = onClick,
                Visible = isVisible
            };

            return toRet;
        }

        private TableCell makeTableCell(params Control[] innerControl)
        {
            TableCell tc = new TableCell();
            foreach (Control control in innerControl)
                tc.Controls.Add(control);
            tc.VerticalAlign = VerticalAlign.Middle;
            return tc;
        }

        private void forceRelease(object sender, EventArgs e)
        {
            Button clicked = (Button) sender;

            hostStateDB.releaseBladeOrVM(clicked.CommandArgument, "console", true);
        }
        
        protected void cmdAddNode_Click(object sender, EventArgs e)
        {
            bladeSpec newBlade = new bladeSpec(txtNewNodeIP.Text, txtNewISCSI.Text, txtNewIloIP.Text, ushort.Parse(txtNewPort.Text), false, null);
            hostStateDB.addNode(newBlade);
        }
    }
}
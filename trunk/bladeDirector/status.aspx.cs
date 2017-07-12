using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Web.UI;
using System.Web.UI.HtmlControls;
using System.Web.UI.WebControls;

namespace bladeDirector
{
    public partial class status : Page
    {
        private static ConcurrentDictionary<string, string> DNSCache = new ConcurrentDictionary<string, string>(); 

        protected void Page_Load(object sender, EventArgs e)
        {
            TableRow headerRow = new TableRow();
            headerRow.Cells.Add(new TableHeaderCell { Text = "" });
            headerRow.Cells.Add(new TableHeaderCell { Text = "State" });
            headerRow.Cells.Add(new TableHeaderCell { Text = "Blade IP" });
            headerRow.Cells.Add(new TableHeaderCell { Text = "Time since last keepalive"});
            headerRow.Cells.Add(new TableHeaderCell { Text = "Currently-selected snapshot" });
            headerRow.Cells.Add(new TableHeaderCell { Text = "Current owner" });
            headerRow.Cells.Add(new TableHeaderCell { Text = "Next owner" });
            headerRow.Cells.Add(new TableHeaderCell { Text = "Links" });
            headerRow.Cells.Add(new TableHeaderCell { Text = "Actions" });

            tblBladeStatus.Rows.Add(headerRow);

            // Get a list of blades without locking any of them
            string[] allBladeIPs = services.hostStateManager.db.getAllBladeIP();
            IEnumerable<bladeSpec> allBladeInfo = allBladeIPs.Select(x => services.hostStateManager.db.getBladeByIP_withoutLocking(x));

            foreach (bladeSpec bladeInfo in allBladeInfo)
            {
                // First, assemble the always-visible status row
                TableRow newRow = new TableRow();

                newRow.Cells.Add(makeTableCell(new ImageButton
                {
                        ImageUrl = "images/collapsed.png",
                        AlternateText = "Details",
                        OnClientClick = "javascript:toggleDetail($(this), " + bladeInfo.bladeID + "); return false;" 
                }));
                newRow.Cells.Add(new TableCell {Text = bladeInfo.state.ToString()});
                newRow.Cells.Add(new TableCell {Text = bladeInfo.bladeIP});
                if (bladeInfo.lastKeepAlive == DateTime.MinValue)
                {
                    newRow.Cells.Add(new TableCell {Text = "(none)"});
                }
                else
                {
                    string cssClass = "";
                    if (DateTime.Now - bladeInfo.lastKeepAlive > services.hostStateManager.keepAliveTimeout)
                        cssClass = "timedout";
                    TableCell cell = new TableCell
                    {
                        Text = formatDateTimeForWeb((DateTime.Now - bladeInfo.lastKeepAlive)),
                        CssClass = cssClass
                    };
                    newRow.Cells.Add(cell);
                }
                newRow.Cells.Add(new TableCell { Text = bladeInfo.currentSnapshot});
                newRow.Cells.Add(new TableCell { Text = getDNS(bladeInfo.currentOwner) ?? "none" });
                newRow.Cells.Add(new TableCell { Text = getDNS(bladeInfo.currentOwner) ?? "none" });

                string iloURL = String.Format("https://ilo-blade{0}.management.xd.lan/", Int32.Parse(bladeInfo.bladeIP.Split('.')[3]) - 100);
                HyperLink link = new HyperLink {NavigateUrl = iloURL, Text = "iLo"};
                TableCell iloURLtableCell = new TableCell();
                iloURLtableCell.Controls.Add(link);
                newRow.Cells.Add(iloURLtableCell);
            
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
            }

            // Finally, populate any log events.
            List<string> logEvents = services.hostStateManager.getLogEvents();
            foreach (string logEvent in logEvents)
                lstLog.Items.Add(logEvent);
        }

        private string getDNS(string toLookUp)
        {
            if (toLookUp == null)
                return null;
            string lookedUp;
            if (DNSCache.TryGetValue(toLookUp, out lookedUp))
            {
                if (lookedUp == null)
                    return toLookUp;
                return lookedUp;
            }

            try
            {
                IPHostEntry entry = Dns.GetHostEntry(IPAddress.Parse(toLookUp));
                lookedUp = entry.HostName;
            }
            catch (Exception)
            {
                lookedUp = null;
            }

            DNSCache.TryAdd(toLookUp, lookedUp);

            return lookedUp;
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
                new Label { Text = "Blade DB ID: " },
                new Label { Text = bladeInfo.bladeID + "<br/>", CssClass = "fixedSize"},
                new Label { Text = "ISCSI IP: "},
                new Label { Text = bladeInfo.iscsiIP + "<br/>", CssClass = "fixedSize" },
                new Label { Text = "Kernel debug port: " },
                new Label { Text = bladeInfo.iLOPort + "<br/>", CssClass = "fixedSize" },
                new Label { Text = "Is currently having BIOS config deployed: " },
                new Label { Text = bladeInfo.currentlyHavingBIOSDeployed + "<br/>", CssClass = "fixedSize" },
                new Label { Text = "Is currently acting as VM server: " },
                new Label { Text = bladeInfo.currentlyBeingAVMServer + "<br/>", CssClass = "fixedSize"}
            ));
            detailTable.Rows.Add(miscTR);

            TableRow pxeScriptRow = new TableRow();
            pxeScriptRow.Cells.Add(makeTableCell(
                makeImageButton("show", "images/collapsed.png", string.Format(@"javascript:toggleConfigBox($(this), ""getIPXEScript.aspx?hostip={0}""); return false;", bladeInfo.bladeIP)),
                new Label { Text = "Current PXE script" },
                makeInvisibleDiv()
                ));
            detailTable.Rows.Add(pxeScriptRow);

            TableRow biosConfigRow = new TableRow();
            biosConfigRow.Cells.Add(makeTableCell(
                makeImageButton("show", "images/collapsed.png", string.Format(@"javascript:toggleConfigBox($(this), ""getBIOSConfig.ashx?hostip={0}""); return false;", bladeInfo.bladeIP)),
                new Label { Text = "Current BIOS configuration" },
                makeInvisibleDiv()
                ));
            detailTable.Rows.Add(biosConfigRow);


            // And add rows for any VMs. Again, avoid locking any of them.
            string[] allVMIPs = services.hostStateManager.db.getAllBladeIP();
            IEnumerable<vmSpec> VMs = allVMIPs.Select(x => services.hostStateManager.db.getVMByIP_withoutLocking(x));
            if (VMs.Any())
            {
                TableRow VMHeaderRow = new TableRow();
                VMHeaderRow.Cells.Add(new TableHeaderCell { Text = "" });
                VMHeaderRow.Cells.Add(new TableHeaderCell { Text = "Child VM name" });
                VMHeaderRow.Cells.Add(new TableHeaderCell { Text = "Child VM IP" });
                VMHeaderRow.Cells.Add(new TableHeaderCell { Text = "iSCSI IP" });
                VMHeaderRow.Cells.Add(new TableHeaderCell { Text = "Current owner" });
                VMHeaderRow.Cells.Add(new TableHeaderCell { Text = "Kernel debug info" });
                //VMHeaderRow.Cells.Add(new TableHeaderCell() { Text = "Current snapshot" });
                detailTable.Rows.Add(VMHeaderRow);
            }
            foreach (vmSpec vmInfo in VMs)
            {
                TableRow thisVMRow = new TableRow();

                thisVMRow.Cells.Add(makeTableCell(
                    makeImageButton("show", "images/collapsed.png", string.Format(@"javascript:toggleConfigBox($(this), ""getIPXEScript.aspx?hostip={0}""); return false;", vmInfo.VMIP)),
                    new Label { Text = "Current PXE script" },
                    makeInvisibleDiv()
                    ));

                thisVMRow.Cells.Add(new TableCell { Text = vmInfo.displayName });
                thisVMRow.Cells.Add(new TableCell { Text = vmInfo.VMIP });
                thisVMRow.Cells.Add(new TableCell { Text = vmInfo.iscsiIP });
                thisVMRow.Cells.Add(new TableCell { Text = vmInfo.currentOwner });
                string dbgStr = String.Format("Port {0} key \"{1}\"", vmInfo.kernelDebugPort, vmInfo.kernelDebugKey) ;
                thisVMRow.Cells.Add(new TableCell { Text = dbgStr });
                //thisVMRow.Cells.Add(new TableCell() { Text = vmInfo.currentSnapshot });
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
            ImageButton toRet = new ImageButton
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

            services.hostStateManager.releaseBladeOrVM(clicked.CommandArgument, "console", true);
        }
        
        protected void cmdAddNode_Click(object sender, EventArgs e)
        {
            bladeSpec newBlade = new bladeSpec(txtNewNodeIP.Text, txtNewISCSI.Text, txtNewIloIP.Text, ushort.Parse(txtNewPort.Text), false, VMDeployStatus.needsPowerCycle,  "bios stuff", bladeLockType.lockAll);
            services.hostStateManager.db.addNode(newBlade);
        }
    }
}
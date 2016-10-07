using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace bladeDirector
{
    public partial class status : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            TableRow headerRow = new TableRow();
            headerRow.Cells.Add(new TableCell() {Text = "State"});
            headerRow.Cells.Add(new TableCell() {Text = "Blade IP"});
            headerRow.Cells.Add(new TableCell() {Text = "Time since last keepalive"});
            headerRow.Cells.Add(new TableCell() {Text = "Current owner"});
            headerRow.Cells.Add(new TableCell() {Text = "Next owner"});

            tblBladeStatus.Rows.Add(headerRow);

            string[] bladeNames = hostStateDB.getAllBladeIP();

            foreach (string bladeName in bladeNames)
            {
                bladeOwnership bladeInfo = hostStateDB.bladeStates.Single(x => x.bladeIP == bladeName);

                TableRow newRow = new TableRow();
                newRow.Cells.Add(new TableCell() {Text = bladeInfo.state.ToString()});
                newRow.Cells.Add(new TableCell() {Text = bladeInfo.bladeIP});
                if (bladeInfo.lastKeepAlive == DateTime.MinValue)
                    newRow.Cells.Add(new TableCell() { Text = "(none)" });
                else
                    newRow.Cells.Add(new TableCell() { Text = (DateTime.Now - bladeInfo.lastKeepAlive).ToString() });
                newRow.Cells.Add(new TableCell() { Text = bladeInfo.currentOwner ?? "none" });
                newRow.Cells.Add(new TableCell() {Text = bladeInfo.nextOwner ?? "none" });

                tblBladeStatus.Rows.Add(newRow);

                List<string> logEvents = hostStateDB.getLogEvents();
                foreach (string logEvent in logEvents)
                    lstLog.Items.Add(logEvent);
            }
        }

        protected void cmdReset_Click(object sender, EventArgs e)
        {
            hostStateDB.resetAll();
        }
    }
}
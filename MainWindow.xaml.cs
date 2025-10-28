using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.Data;
using System.Data.SqlClient;
using System.Windows.Threading;

namespace TableTransferApp
{
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
        }

        private void ShowError(string message)
        {
            errMsg.Text = message;
            errMsg.Foreground = Brushes.Red;
            errMsg.Visibility = Visibility.Visible;
        }

        private void ShowInfo(string message)
        {
            errMsg.Text = message;
            errMsg.Foreground = Brushes.Green;
            errMsg.Visibility = Visibility.Visible;
        }

        private void ClearMessageNow()
        {
            var tb = errMsg as System.Windows.Controls.TextBlock;
            if (tb != null)
            {
                tb.Text = string.Empty;
            }

            errMsg.Visibility = Visibility.Collapsed;

            Dispatcher.Invoke(() => { }, DispatcherPriority.Render);
        }

        private void execTransferBtn_Click(object sender, RoutedEventArgs e)
        {
            ClearMessageNow();

            if (string.IsNullOrWhiteSpace(txtSourceConnectionString.Text) ||
                string.IsNullOrWhiteSpace(txtSourceDatabase.Text) ||
                string.IsNullOrWhiteSpace(txtSourceTable.Text) ||
                string.IsNullOrWhiteSpace(txtDestConnectionString.Text) ||
                string.IsNullOrWhiteSpace(txtDestDatabase.Text) ||
                string.IsNullOrWhiteSpace(txtDestTable.Text))
            {
                ShowError("All fields are required. Please fill in every input.");
                return;
            }

            errMsg.Visibility = Visibility.Collapsed;

            try
            {
                var tt = new TableTransfer
                {
                    SourceConnectionString = txtSourceConnectionString.Text.Trim(),
                    SourceDatabase = txtSourceDatabase.Text.Trim(),
                    SourceTable = txtSourceTable.Text.Trim(),
                    DestConnectionString = txtDestConnectionString.Text.Trim(),
                    DestDatabase = txtDestDatabase.Text.Trim(),
                    DestTable = txtDestTable.Text.Trim()
                };

                int rows = tt.ExecuteTransfer();

                ShowInfo(
                    $"Success: {rows} rows copied from [{tt.SourceDatabase}].[{tt.SourceTable}] " +
                    $"to [{tt.DestDatabase}].[{tt.DestTable}] in a single transaction.");
            }
            catch (ArgumentException aex)
            {
                ShowError(aex.Message);
            }
            catch (SqlException sqlex)
            {
                ShowError("Database error: " + sqlex.Message);
            }
            catch (Exception ex)
            {
                ShowError("Unexpected error: " + ex.Message);
            }
        }
    }
}

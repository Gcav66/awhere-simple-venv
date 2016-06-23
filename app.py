from flask import Flask, request, redirect, render_template, send_file
from AWhere_Update import AwhereUpdate
import pandas as pd
import os


def create_app():
    app=Flask(__name__)
    return app

app = create_app()

#with open("test_credentials.txt") as f:
    #data = f.read().split("\n")
    #key = data[0].strip()
    #secret = data[1].strip()
    


def get_single_data(akey, asec, mlat, mlong, sdate, edate):
    client = AwhereUpdate(akey, asec)
    response = client.single_call(mlat, mlong, sdate, edate)
    clean = client.flatten_single(response)
    df = pd.DataFrame(clean)
    #xls_name = mlat + "_" + mlong + "_" + sdate + "_" + edate + ".xlsx"
    xls_name = "GDA_AWHERE_Weather.xlsx"
    df_xls = df.to_excel(xls_name, index=False)
    return xls_name

def get_forecast(akey, asec, mlat, mlong, sdate, edate):
    client = AwhereUpdate(akey, asec)
    response = client.single_forecast(mlat, mlong, sdate, edate)
    flat = client.flatten_forecast(response)
    df = pd.DataFrame(flat)
    forecast_order = ['startTime', 'endTime', 'temperature_max', 'temperature_min',
                  'temperature_unit', 'precipitation_amount', 'precipitation_chance',
                  'precipitation_units', 'precipitation_chance', 'wind_average',
                  'wind_max', 'wind_min', 'wind_units', 'sky_sunshine', 'solar_amount',
                  'solar_units']
    ordered_df = df[forecast_order]
    xls_name = "GDA_AWHERE_Forecast_Weather.xlsx"
    df_xls = ordered_df.to_excel(xls_name, index=False)
    return xls_name

@app.route("/", methods=['GET', 'POST'])
def upload():
    template = 'upload_file.html'
    if request.method == 'POST':
        if request.form['btn'] == 'observation':
            akey = request.form['api_key']
            asec = request.form['api_secret']
            mlat = request.form['latitude_input']
            mlong = request.form['longitude_input']
            sdate = request.form['start_date']
            edate = request.form['end_date']
            xls_name = get_single_data(akey, asec, mlat, mlong, sdate, edate)
            return send_file(xls_name, as_attachment=True)
        if request.form['btn'] == 'forecast':
            akey = request.form['api_key']
            asec = request.form['api_secret']
            mlat = request.form['latitude_input']
            mlong = request.form['longitude_input']
            sdate = request.form['start_date']
            edate = request.form['end_date']
            xls_name = get_forecast(akey, asec, mlat, mlong, sdate, edate)
            return send_file(xls_name, as_attachment=True)

        """
        myfile = request.files['inputFile']
        myfile.save(myfile.filename)
        df = pd.DataFrame(myfile.filename)
        return send_file("foo.xlsx", as_attachment=True)
        """
    return render_template(template)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=int(port), debug=True)

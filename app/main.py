###################
#      full       #    
#  v9 may 2021    #
#                 #
###################

import pandas as pd
import numpy as np
import json
import psycopg2 
import os
import warnings
warnings.filterwarnings('ignore')

from bokeh.io import curdoc
from bokeh.plotting import figure
from bokeh.layouts import row, column
from bokeh.models import (HoverTool, Panel, NumeralTickFormatter, 
                          ColumnDataSource, Div, BoxSelectTool)
from bokeh.models.widgets import CheckboxGroup, Tabs, Select, RadioButtonGroup


def connect():
    """
    Query all data from Postgres DB 
    Return df
    """
    DATABASE_URL = os.environ['DATABASE_URL']

    with psycopg2.connect(DATABASE_URL, sslmode = 'require') as conn:
        q = """
        SELECT * FROM country;
        
        """
        
        cur = conn.cursor()
        cur.execute(q)
        data = cur.fetchall()
        conn.commit()   
    cols = [i[0] for i in cur.description]
    df = pd.DataFrame(data, columns = cols)
    df.set_index('Date', inplace =True)
    df = df.astype('float')
    df.columns.name = 'Columns'
    df.index.name = 'Date'
    df.index = pd.to_datetime(df.index)
    return df          
        
def make_data(selection_sum, colour):

    global radiob_act6
    
    '''
    Creates a column dataset based on interactive filter selection
    Calculates rolling average trenline for daily graphs.
    Returns ColumnDataSource.data.keys(): index, Date, Country, value series
    '''
    
    
    # create list of column names form selection
    ##remove 'No selection' from the list if there are others selected
    if len(selection_sum) >1 :
        selection_sum = [i for i in selection_sum if i != 'No selection']
    else : 
        pass

    sel_daily = [i+'_daily' for i in set(selection_sum)]
    sel_dead = [i+'_dead' for i in set(selection_sum)]
    sel_daily_dead = [i+'_dead' for i in sel_daily]
    sel_daily_roll = [i+'_roll' for i in sel_daily]
    sel_daily_dead_roll = [i+'_roll' for i in sel_daily_dead]

    df = sick[selection_sum + sel_daily + sel_dead+ sel_daily_dead]
        
    # trendline
    days = 7
    for i in sel_daily:
        df[i+'_roll'] = df[i].rolling(window = days).mean()
        df[i+'_dead_roll'] = df[i+'_dead'].rolling(window = days).mean()
    
    #normalise with 100.000 population
    if radiob_act6 != 0:

        temp = pd.DataFrame()
        for i in selection_sum:

            n = (pop['data'][i])
            temp1 = df.loc[:,[ii for ii in df.columns if i in ii]] / (n)
            temp = pd.concat((temp, temp1), axis = 1) 
            s_cds = temp
        del temp
    else: s_cds = df.copy()
    
    del df
    
    s_cds.reset_index(inplace= True)
    s_cds['Date'] = pd.to_datetime(s_cds['Date'])
    
    #melts and combine sum values and diff values to a single df
    a1 = pd.melt(s_cds, id_vars = 'Date', value_vars = selection_sum, var_name = 'Country' )
    a2 = pd.melt(s_cds, id_vars = 'Date', value_vars = sel_daily, var_name = 'Country',value_name = 'value_day' )
    a3 = pd.melt(s_cds, id_vars = 'Date', value_vars = sel_dead, var_name = 'Country',value_name = 'value_dead' )
    a4 = pd.melt(s_cds, id_vars = 'Date', value_vars = sel_daily_dead, var_name = 'Country',value_name = 'value_day_dead' )
    a5 = pd.melt(s_cds, id_vars = 'Date', value_vars = sel_daily_roll, var_name = 'Country', value_name = 'value_day_roll')
    a6 = pd.melt(s_cds, id_vars = 'Date', value_vars = sel_daily_dead_roll, var_name = 'Country', value_name = 'value_day_dead_roll')

    
    s_cds = pd.concat([a1,a2['value_day'],
                       a3['value_dead'],
                       a4['value_day_dead'],
                       a5['value_day_roll'],
                       a6['value_day_dead_roll']], axis = 1, ignore_index= True)
    s_cds.columns = ['Date', 'Country', 'value','value_day','value_dead','value_day_dead','value_day_roll','value_day_dead_roll']

    #get rid of empty dates except if 'No selection' is there
    s_cds = s_cds.applymap(lambda x: np.NaN if x == 0 else x)
    if 'No selection' not in s_cds['Country'].unique():
        s_cds.dropna(axis = 1, how = 'all', inplace = True)

    # colourise
    s_cds['colour'] = s_cds['Country'].map(colour)
    
    return s_cds

def make_plot(src):
    
    global radiob_act6
    
    '''
    Create plot according to fed in cds
    ''' 
    if radiob_act6 == 0:
        hover_fig = {'Date': '@Date{%Y-%m-%d}'
                    ,'Country': '@Country'
                    ,'Infected': '@value{,000,000}'}
        hover_fig1 = {'Date': '@Date{%Y-%m-%d}'
                    ,'Country': '@Country'
                    ,'Infected': '@value_day{0}'}
        hover_fig2 = {'Date': '@Date{%Y-%m-%d}'
                    ,'Country': '@Country'
                    ,'Dead': '@value_dead{0}'}
        hover_fig3 = {'Date': '@Date{%Y-%m-%d}'
                    ,'Country': '@Country'
                    ,'Dead': '@value_day_dead{0}'}
    else:
        hover_fig = {'Date': '@Date{%Y-%m-%d}'
                ,'Country': '@Country'
                ,'Infected': '@value{0}'}
        hover_fig1 = {'Date': '@Date{%Y-%m-%d}'
                    ,'Country': '@Country'
                    ,'Infected': '@value_day{0.0000}'}
        hover_fig2 = {'Date': '@Date{%Y-%m-%d}'
                    ,'Country': '@Country'
                    ,'Dead': '@value_dead{0}'}
        hover_fig3 = {'Date': '@Date{%Y-%m-%d}'
                    ,'Country': '@Country'
                    ,'Dead': '@value_day_dead{0.0000}'}

    fig_kwargs = {
                  'plot_width': 700
                  ,'plot_height': 200
                  ,'x_axis_type': 'datetime'
                  ,'background_fill_color': '#f1f5fc'
                  ,'border_fill_color' : '#f1f5fc'
                  ,'tools' :['pan','box_zoom', 'reset']
                  }
    plot_kwargs = {'source': src
                   ,'hover_fill_color':'red'
                   ,'hover_color':'red'
                   ,'hover_alpha': 1
                   ,'color' : 'colour'
                   ,'size': 6
                   ,'selection_color':'red'
                   ,'nonselection_color':'colour'
                  }
    line_kwargs = {'color' : 'black'
                   ,'source':src
                   ,'line_width': 0.5   
                    }
    box = BoxSelectTool(dimensions='width')

    fig = figure(**fig_kwargs 
                ,title = 'Total Infection'
                ,toolbar_location = 'above'
                )
    fig.diamond(x='Date',y='value', **plot_kwargs, legend='Country')

    fig.add_tools(HoverTool(tooltips=hover_fig,
                            formatters ={'@Date': 'datetime', '@value': 'numeral'},
                            mode = 'vline'),
                  box)
    fig.toolbar.active_drag=box
    fig.toolbar.logo = None
    fig.legend.location = 'top_left'
    fig.legend.label_text_font_size = '10px'
    fig.legend.glyph_height = 10
    fig.legend.label_height = 10
    fig.legend.label_text_line_height = 0.5
    fig.legend.spacing = 1
    fig.yaxis.formatter=NumeralTickFormatter(format=",000,000") #format number display
    fig.ygrid.band_fill_color="olive"  #creates band colouring
    fig.ygrid.band_fill_alpha = 0.1
  ##################<<<<<<<<<<<<>>>>>>>>>>#################  
    fig1 = figure(**fig_kwargs
                 ,title = 'Daily infection'
                 )
    fig1.diamond(x='Date',y='value_day', **plot_kwargs)
    fig1.vline_stack(x='Date', stackers='value_day_roll', **line_kwargs)
    fig1.x_range = fig.x_range
    fig1.add_tools(HoverTool(tooltips=hover_fig1,
                            formatters ={'@Date': 'datetime', '@value_day': 'numeral'},
                            mode = 'mouse'),
                   box)
    fig1.toolbar.active_drag=box
    fig1.toolbar.logo = None
    fig1.yaxis.formatter=NumeralTickFormatter(format=",000,000") #format number display
    fig1.ygrid.band_fill_color="olive"  #creates band colouring
    fig1.ygrid.band_fill_alpha = 0.1
  ##################<<<<<<<<<<<<>>>>>>>>>>#################  
    fig2 = figure(**fig_kwargs
                 ,title = 'Total death')
    fig2.diamond(x='Date',y='value_dead', **plot_kwargs)
    fig2.x_range = fig.x_range
    fig2.add_tools(HoverTool(tooltips=hover_fig2,
                            formatters ={'@Date': 'datetime', '@value_dead': 'numeral'},
                            mode = 'vline'),
                   box)
    fig2.toolbar.active_drag=box
    fig2.toolbar.logo = None
    fig2.yaxis.formatter=NumeralTickFormatter(format=",000,000") #format number display
    fig2.ygrid.band_fill_color="olive"  #creates band colouring
    fig2.ygrid.band_fill_alpha = 0.1            
  ##################<<<<<<<<<<<<>>>>>>>>>>#################  
    fig3 = figure(**fig_kwargs
                 ,title = 'Daily death')
    fig3.diamond(x='Date',y='value_day_dead', **plot_kwargs)
    fig3.line(x='Date', y = 'value_day_dead_roll', **line_kwargs)
    fig3.x_range = fig.x_range
    fig3.add_tools(HoverTool(tooltips=hover_fig3,
                            formatters ={'@Date': 'datetime', '@value_day_dead': 'numeral'},
                            mode = 'mouse'),
                   box)
    fig3.toolbar.active_drag=box
    fig3.toolbar.logo = None
    fig3.yaxis.formatter=NumeralTickFormatter(format=",000,000") #format number display
    fig3.ygrid.band_fill_color="olive"  #creates band colouring
    fig3.ygrid.band_fill_alpha = 0.1                 


    layout1 =column(fig,fig2,fig1,fig3)
    layout2 =row(column(fig,fig2),column(fig1,fig3))
    return layout1,layout2

def update(attr, old, new):
    
    global radiob_act6
    
    chkbox_act1 = [checkbox_group1.labels[i] for i in checkbox_group1.active]
    chkbox_act2 = [checkbox_group2.labels[i] for i in checkbox_group2.active]
    drpbox_act3 = [drop_box_group3.value]
    drpbox_act4 = [drop_box_group4.value]
    drpbox_act5 = [drop_box_group5.value]
    radiob_act6 = radiobuttgroup6.active
    selection_sum = chkbox_act1+chkbox_act2+drpbox_act3+drpbox_act4+drpbox_act5
    selection_sum = set(selection_sum)
    
    #keeping existing colours
    colour_list = ['#1f77b4','#ff7f0e', '#2ca02c', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22','#17becf']
    old_colour = {i:c for i,c in zip(src.data['Country'],src.data['colour'])}

    colour = {}
    #remove active coloiurs and add to the new colour dict
    for i in selection_sum:
        if i in old_colour.keys():
            colour[i] = old_colour[i]
            colour_list.remove(old_colour[i])
    #add new countries to the colour dict
    for i, n in enumerate(selection_sum):
        if n not in colour.keys():
            colour[n] = colour_list[0]
    
    new_src_0 = make_data(selection_sum, colour)  
    new_src = ColumnDataSource(new_src_0)          
    src.data.update(new_src.data)
    
    #fatality update
    selected = src.selected.indices
    if selected:
        df = new_src_0.iloc[selected, :]
        
        stats.text = fatality(df, selected)
    else:
        stats.text = fatality(new_src_0)
    
    
    del new_src
    return src

    
def fatality(df, *args):
    
    if args:
        args = 'between '+str(df.iloc[0,0].date())+' - '+str(df.iloc[-1,0].date())
        
    else: 
        args = 'full time period'    
    f = {}
    
    for i in df.Country.unique():
        v = df.loc[df['Country'] == i, ['value_day', 'value_day_dead']]
        f[i] = round(v['value_day_dead'].sum()*100/ v['value_day'].sum(), 1)
        
    # header
    b = """<span style="font-weight: bold;">
    Fatality
    <span title='Select the time period with the box selection. Before changing countries Reset/ESC selection'
    style="text-decoration: underline dotted;">
     <sup>?</sup></span> : {}</span>
    <br>""".format(args)
    
    # cols of calulated fatality
    n = 1
    for k,v in f.items():
        if n%3 == 0:
            a = '<div style="display:inline-block; width:250px"><b>{}</b>: {}%</div><br>'.format(k,v)
            b = b+a
        else: 
            a= '<div style="display:inline-block; width:250px"><b>{}</b>: {}%</div>'.format(k,v)
            b = b+a
        n += 1
    
    return b
    
# getting the data

sick = connect()
pop = json.load(open('population_covid.json'))
col_list = [ i for i in sick.columns if 'dead' not in i and 'daily' not in i]
colour_list = ['#1f77b4','#ff7f0e', '#2ca02c', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22','#17becf']

#make selection list excuding checkbox values
dropdown_list = ['No selection'] + [i for i in col_list \
                if i not in ['No selection', 'World', 'China', 'US', 'United Kingdom','India','Italy']]

drop_width = 110
chk_width = 110
    
checkbox_group1 = CheckboxGroup(labels=['World','China','US'], active=[0], width=chk_width)
checkbox_group2 = CheckboxGroup(labels=['United Kingdom','Italy','India'], active= [0], width=chk_width)
drop_box_group3 = Select(title="Select country",value='No selection',options=dropdown_list, width=drop_width)
drop_box_group4 = Select(title="Select country",value='No selection',options=dropdown_list,width=drop_width)
drop_box_group5 = Select(title="Select country",value='No selection',options=dropdown_list,width=drop_width)
radiobuttgroup6 = RadioButtonGroup(labels=['Absolute', 'Normalized'], active=1, width=80)

checkbox_group1.on_change('active', update)
checkbox_group2.on_change('active', update)
drop_box_group3.on_change('value', update)
drop_box_group4.on_change('value', update)
drop_box_group5.on_change('value', update)
radiobuttgroup6.on_change('active', update)


ctrl1 = column(checkbox_group1) 
ctrl2 = column(checkbox_group2)
ctrl3 = column(drop_box_group3)
ctrl4 = column(drop_box_group4)
ctrl5 = column(drop_box_group5)
ctrl6 = column(radiobuttgroup6)


start_sel1 = [checkbox_group1.labels[i] for i in checkbox_group1.active]
start_sel2 = [checkbox_group2.labels[i] for i in checkbox_group2.active]
start_sel3 = [drop_box_group3.value]
start_sel4 = [drop_box_group4.value]
start_sel5 = [drop_box_group5.value]
radiob_act6 = radiobuttgroup6.active
start_sum  = start_sel1 + start_sel2 +start_sel3 + start_sel4 + start_sel5
colour = {'World':'#1f77b4', 'United Kingdom':'#ff7f0e'}

src_0 = make_data(start_sum, colour)
src = ColumnDataSource(src_0)
fig = make_plot(src)

src.selected.on_change('indices', update)

stats = Div(text=fatality(src_0), width=750)

coLayout = column(row(ctrl1,ctrl2, ctrl3, ctrl4,ctrl5, ctrl6),row(stats),fig[0])
roLayout = column(row(ctrl1,ctrl2, ctrl3, ctrl4,ctrl5, ctrl6),row(stats),fig[1])

colPanel = Panel(child = coLayout, title = 'Phone Layout')
rowPanel = Panel(child = roLayout, title = 'Monitor Layout')
tabs = Tabs(tabs=[colPanel,rowPanel], background='#f1f5fc')


curdoc().add_root(tabs)


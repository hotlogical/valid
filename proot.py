# (c) 2009 - 2022 E. Cawley. All Rights Reserved.

# Abandon all hope ye who enter here
# This needs a total re-write - absolutely no warranty on any of this

import gc
import os
# from DataHelper import DataHelper

from array import array

from IPython.display import display_pretty, display_png, display_svg, Image, SVG, display

from ROOT import TGraph, TGraphErrors, TMultiGraph, TCanvas, TLine, TH1D, TH2D, TF1, TProfile, TDatime, TLegend, TText
from ROOT import gStyle, gROOT, gSystem, gPad, gDirectory, SetOwnership, TColor, TPad, TFile, TDirectory, TMarker, TBox
from ROOT import TPave, TPaveText, THttpServer, nullptr, TImage, TBufferJSON
from ROOT import kWhite, kGray, kBlack, kYellow, kOrange, kRed, kPink, kMagenta, kViolet, kBlue, kAzure, kCyan, kTeal, \
    kGreen, kSpring
from ROOT import kDot, kBlue, kStar, kCircle, kMultiply, kFullDotSmall, kFullDotLarge, kFullCircle
from ROOT import kFullSquare, kFullTriangleUp, kFullTriangleDown, kOpenCircle, kOpenSquare, kOpenTriangleUp
from ROOT import kOpenTriangleDown, kOpenDiamond, kOpenCross, kFullStar, kOpenStar, kFullDiamond, kFullCross
# from ROOT import Double as PassByRef
import numpy as np
import pandas as pd


# os.environ['DISPLAY'] = 'ipaddress'


class ECanvas:

    def __init__(self, name='c', inline=True, width=700, height=700, topx=1800, topy=0):
        self.name = name
        self.width = width
        self.height = height
        self.topx = topx
        self.topy = topy
        self.inline = inline
        self.c = self.createCanvas(inline)

    def createCanvas(self, inline):
        if inline: self.height = int(self.height * 0.5)
        c = TCanvas(self.name, self.name, self.topx, self.topy, self.width, self.height)
        SetOwnership(c, False)
        # c.SetBatch(inline)  # TODO fix this !
        return c

    def popin(self, force=None):
        inline = None
        if force is None:
            inline = not self.inline
        else:
            inline = force
        print('popin', self.inline, inline)
        if self.inline != inline:
            print('toggling popin for', self.name)
            if not inline:
                print('gROOT.SetBatch(False) %s')
                gROOT.SetBatch(False)
            d = self.c.DrawClone()
            self.c.Destructor()
            d.SetName(self.name)
            d.SetTitle(self.name)
            if not inline:
                self.height *= 2
            else:
                self.height = int(self.height * 0.95)
            if not inline:
                d.SetWindowPosition(self.topx, self.topy)
                d.SetWindowSize(self.width, self.height)
            if not inline:
                print('gROOT.SetBatch(True)')
                gROOT.SetBatch(True)
            self.c = d
            SetOwnership(self.c, False)
            self.inline = inline
            gc.collect()

    def getCanvas(self):
        return self.c

    def isinline(self):
        return self.inline

    def update(self):
        self.c.Modified()
        self.c.Update()

    def cd(self):
        self.c.cd()

    def getName(self):
        return self.name

    def getInline(self):
        return self.inline

    def show(self, out=''):
        if not self.inline: return
        if not out: out = 'png'
        if out == 'png':
            self.c.Print('graph.png', 'png')
            p = Image('graph.png')
            display(p)
            return p
        if out == 'svg':
            self.c.Print('graph.svg', 'svg')
            p = SVG('graph.svg')
            display(p)
            return p
        if out == 'st':
            #timg = TImage.Create()
            #timg.FromPad(self.c)
            #timg.WriteImage('graph.png')
            poo = None
            #buf = TImageBuffer()
            #timg.GetImageBuffer(buf, buf.BufferSize())
            #### a = self.c.Print('graph.png', 'png')
            #### f = TFile().Open('graph.root', "RECREATE")
            #### self.c.Write()
            #### f.Close()
            # js = TBufferJSON.ExportToFile('graph.json', self.c)  # .Data()
            js = TBufferJSON.ConvertToJSON(self.c, 23).Data()  # .replace('\n', '')
            #with open('graph.json', 'w') as fh:
            #    fh.write(js)
            # p = Image('graph.png')
            # display(p)
            return js

    # def __del__(self):
    #    print ('self.c ', self.c)
    #    if self.c is not None:
    #        print('why')
    #        self.c.Destructor()
    #        gc.collect()


class CanvasManager:

    def __init__(self, allinline=True):
        self.allinline = allinline
        self.canvases = {}
        self.areInLine = {}
        self.currcanvas = None

    def setAllInline(self, allinline):
        for c in self.canvases:
            self.canvases[c].popin(c, allinline)
        self.allinline = allinline

    def getCanvas(self, name='c', inline=None, width=700, height=400, topx=1800, topy=0):
        if inline is None:
            inline = self.allinline
        if name in self.canvases:
            return self.canvases[name]
        # print('gROOT.SetBatch(inline) %s' % inline)
        gROOT.SetBatch(inline)
        self.canvases[name] = ECanvas(name, inline, width, height, topx, topy)
        if inline and not self.allinline:
            print('gROOT.SetBatch(False) inline %s allinline %s' % (inline, self.allinline))
            gROOT.SetBatch(False)
        self.currcanvas = name
        return self.canvases[name].getCanvas()

    def delCanvas(self, name='c'):
        if not name in self.canvases:
            print('No canvas with name %s' % name)
            return
        del self.canvases[name]

    def popin(self, name, force=None):
        if not name in self.canvases:
            return
        self.canvases[name].popin(force)

    def cd(self, name):
        if not name in self.canvases:
            print('\nNo canvas with name %s' % name)
            return
        self.canvases[name].cd()

    def listCanvases(self):
        print('Name\tInline')
        for c in sorted(self.canvases):
            isCurrent = ''
            if c == self.currcanvas: isCurrent = 'Selected'
            print('%s\t%s\t%s' % (c, self.canvases[c].getInline(), isCurrent))

    # def __del__(self):
    #    for c in list(self.canvases.keys()):
    #        del self.canvases[c]
    #    gc.collect()


class pROOT:

    def __init__(self, inline=False, dark=None):

        self.gr = gROOT
        if inline:
            print('gROOT.SetBatch(inline) %s' % inline)
            self.gr.SetBatch(True)
        self.cm = CanvasManager(inline)
        self.canvas = None
        self.c = None
        self.things = []
        self.st = gStyle
        self.TH1D = TH1D
        self.TF1 = TF1
        self.TFile = TFile
        self.TPad = TPad
        self.TCanvas = TCanvas
        self.TDirectory = TDirectory
        self.THttpServer = THttpServer
        self.TDatime = TDatime
        self.gDirectory = gDirectory
        self.gROOT = gROOT
        self.gSystem = gSystem
        #        self.PassByRef = PassByRef
        self.cols = {}
        self.setDefStyle()
        if dark is not None:
            self.setDark()
        self.TLegend = TLegend
        self.TGraph = TGraph
        self.TMultiGraph = TMultiGraph

    def setDefStyle(self):
        st = self.st
        st.SetOptStat(0)
        st.SetOptFit(0)
        st.SetNumberContours(50)
        st.SetOptDate(22)
        st.GetAttDate().SetTextSize(0.02)
        st.GetAttDate().SetTextFont(82)
        self.defaultPalette = 56
        st.SetPalette(self.defaultPalette)
        tmp1 = 'W G B y o r p m v b a c t g s'.split()
        tmp2 = [kWhite, kGray, kBlack, kYellow, kOrange, kRed + 1, kPink, kMagenta + 1, kViolet, kBlue,
                kAzure, kCyan, kTeal, kGreen + 1, kSpring]
        self.cols = dict(zip(tmp1, map(str, tmp2)))

    def setDark(self):
        st = self.st
        st.SetAxisColor(kGreen+4, "xyz")  # Second val is x or y
        st.SetCanvasColor(1)
        st.SetFrameFillColor(1)
        st.SetFrameLineColor(kGreen+4)
        st.SetGridColor(4)
        st.SetHistFillColor(1)
        st.SetHistLineColor(5)
        st.SetLabelColor(0, "xyz")
        st.SetLegendFillColor(1)
        st.SetPadColor(1)
        st.SetStatColor(1)
        st.SetStatTextColor(3)
        st.SetTitleColor(3, "xyz")
        st.SetTitleFillColor(4)
        st.SetTitleTextColor(0)

    def marker(self, x, y, val, d=True):
        timc = 0.000000001
        if isinstance(x, pd.Timestamp):
            x = x.value * timc
        if isinstance(x, str):
            x = self.timeStr2root(x)

        m = TMarker()
        m.SetX(x)
        m.SetY(y)
        self.parseMarkerAtts(m, val)
        m.Draw()
        if d:
            self.update()
        self.things.append(m)
        # gc.collect()
        return m

    def setMarker(self, robj, val):
        self.parseMarkerAtts(robj, val)

    def parseMarkerAtts(self, robj, val):
        # Type, Size, Colour
        if ':' in val:
            vals = val.split(':')
            vlen = len(vals)
            if vlen >= 1:
                markerstr = vals[0]
                if markerstr == '':
                    markernum = 1
                elif markerstr[0] == 'k':
                    markernum = eval(markerstr)
                else:
                    markernum = int(markerstr)
                print('Setting marker style ', markernum)
                robj.SetMarkerStyle(markernum)
            if vlen >= 2:
                msizestr = vals[1]
                if msizestr == '':
                    msizenum = 1.
                else:
                    msizenum = float(msizestr)
                print('Setting marker size ', msizenum)
                robj.SetMarkerSize(msizenum)
            if vlen >= 3:
                mcolstr = vals[2]
                if mcolstr == '':
                    mcolnum = robj.GetLineColor()
                elif mcolstr[0] == 'k':
                    mcolnum = eval(mcolstr)
                elif mcolstr.isalpha():
                    mcolnum = eval(self.cols[mcolstr])
                else:
                    mcolnum = int(mcolstr)
                print('Setting marker colour %s - %s' % (mcolstr, mcolnum))
                robj.SetMarkerColor(mcolnum)

    def setdisp(self, ip=False):
        gSystem.SetDisplay()

    def createCanvas(self, name='c', inline=None, width=700, height=700, topx=100, topy=100):
        self.c = self.cm.getCanvas(name, inline, width, height, topx, topy)
        self.c.SetGrid()
        self.canvas = name
        return self.c

    def setCanvas(self, name):
        if not name in self.cm.canvases:
            print('No canvas with name%s' % name)
            return
        self.canvas = name
        self.cm.cd(name)

    def popin(self, force=None):
        self.cm.popin(self.canvas, force=None)

    def cc(self, name='c'):
        if name not in self.cm.canvases:
            print('No canvas with name %s' % name)
            return
        self.canvas = name
        self.cm.cd(name)
        self.c = self.cm.getCanvas(name).getCanvas()
        self.c.cd()
        return self.c

    def listCanvases(self):
        self.cm.listCanvases()

    def update(self):
        return self.cm.getCanvas(self.canvas).update()

    def isinline(self):
        return self.cm.getCanvas(self.canvas).isinline()

    def show(self, out):
        return self.cm.getCanvas(self.canvas).show(out)

    def clear(self):
        self.things = []
        gc.collect()

    def dttosecs(self, dt):
        return float(dt.value / 1000000000.)

    def timeStr2root(self, t):
        h, m, s = t.split(':')
        import datetime
        tim = datetime.datetime.combine(datetime.date.today(), datetime.time(int(h), int(m), int(s)))
        epoch = datetime.datetime(1970, 1, 1)
        return (tim - epoch).total_seconds()

    def setLineCol(self, robj, val, alpha=1.):
        col = val.strip()[0]
        val = val.replace(col, self.cols[col])
        thiscol = eval(val)
        # robj.SetLineColorAlpha(thiscol, alpha)
        robj.SetLineColor(thiscol)
        robj.SetMarkerColor(thiscol)

    def setFillCol(self, robj, val, alpha=1.):
        col = val.strip()[0]
        val = val.replace(col, self.cols[col])
        thiscol = eval(val)
        # robj.SetLineColorAlpha(thiscol, alpha)
        robj.SetFillColor(thiscol)

    def hline(self, y, x1, x2, c='kGreen', w=1, s=1, a=1., can=None, d=True):
        return self.line(x1, y, x2, y, c, w, s, a, can, d)

    def vline(self, x, y1, y2, c='kGreen', w=1, s=1, a=1., can=None, d=True):
        return self.line(x, y1, x, y2, c, w, s, a, can, d)

    def line(self, x1, y1, x2, y2, c='kGreen', w=1, s=1, a=1., can=None, d=True):

        if can is not None:
            self.cc(can)
        x1 = self.maybeTime(x1)
        x2 = self.maybeTime(x2)

        line = TLine(x1, y1, x2, y2)
        # self.setLineCol(line, c, a)
        mcolstr = c
        if mcolstr == '':
            mcolstr = line.GetLineColor()
        self.setLineAtts(line, mcolstr, w, s, a)

        line.Draw()
        if d:
            self.update()
        self.things.append(line)
        # gc.collect()
        return line

    def maybeTime(self, t):
        timc = 0.000000001
        if isinstance(t, pd.Timestamp):
            t = t.value * timc
        if isinstance(t, str):
            t = self.timeStr2root(t)
        return t

    def setLineAtts(self, obj, color, width, style, alpha):
        color = self.parseLineColor(color)
        # obj.SetLineColorAlpha(color, alpha)
        obj.SetLineColor(color)
        obj.SetLineWidth(width)
        obj.SetLineStyle(style)

    def setFillAtts(self, obj, color, style, alpha):
        # cbef = color
        color = self.parseLineColor(color)
        # print(cbef, color)
        # obj.SetFillColorAlpha(color, alpha)
        obj.SetFillColor(color)
        obj.SetFillStyle(style)

    def setTextAtts(self, text, size, color, alpha, align, angle, font):
        if size is not None:
            text.SetTextSize(size)
        color = self.parseLineColor(color)
        # text.SetTextColorAlpha(color, alpha)
        text.SetTextColor(color)
        if align is not None:
            text.SetTextAlign(align)
        if angle is not None:
            text.SetTextAngle(angle)
        if font is not None:
            text.SetTextFont(font)

    def parseLineColor(self, color):
        if isinstance(color, int):
            return color
        if isinstance(color, float):
            return int(color)
        if isinstance(color, str):
            if len(color) > 1 and color[0] == 'k':
                return eval(color)
            if color.isalpha() and len(color) == 1:
                return eval(self.cols[color])
        return 0

    def box(self, x1, y1, x2, y2, f='kWhite', l='kGreen', w=1, s=1, fs=1001, a=1., fa=1., can=None,
            o='L', d=True):

        x1 = self.maybeTime(x1)
        x2 = self.maybeTime(x2)
        box = TBox(x1, y1, x2, y2)
        self.setLineAtts(box, l, w, s, a)
        self.setFillAtts(box, f, fs, fa)

        box.Draw(o)
        if d:
            self.update()
        self.things.append(box)
        # gc.collect()
        return box

    def pave(self, x1, y1, x2, y2, f='kWhite', l='kGreen', w=1, s=1, fs=1001, a=1., fa=1., can=None,
             o='NB', r=0.2, d=True, n=None):

        x1 = self.maybeTime(x1)
        x2 = self.maybeTime(x2)
        box = TPave(x1, y1, x2, y2, w, o)
        if n is not None:
            box.SetName(n)
        self.setLineAtts(box, l, w, s, a)
        self.setFillAtts(box, f, fs, fa)

        box.Draw(o)
        if d:
            self.update()
        self.things.append(box)
        # gc.collect()
        return box

    def text(self, x, y, string='', s=None, c=1, a=1., al=None, an=None,
             f=None, d=True, n=None, nd=True):

        x = self.maybeTime(x)
        text = TText(x, y, string)
        if n is not None:
            text.SetName(n)
        self.setTextAtts(text, s, c, a, al, an, f)
        if nd:
            text.SetNDC(True)
            text.DrawTextNDC(x, y, string)
        text.Draw()
        if d:
            self.update()
        self.things.append(text)
        # gc.collect()
        return text

    def ptext(self, x1, y1, x2, y2, string='', s=None, n=None, fi='kWhite', l='kGreen', w=1, st=1, fs=1001,
              a=1., al=None, an=None, f=None, fa=1., can=None, o='NB', r=0.2, c=1, d=True):

        x1 = self.maybeTime(x1)
        x2 = self.maybeTime(x2)
        box = TPaveText(x1, y1, x2, y2, o)
        if n is not None:
            box.SetName(n)
        lines = string.split('\n')
        for line in lines:
            box.AddText(line)
        self.setLineAtts(box, l, w, st, a)
        self.setFillAtts(box, fi, fs, fa)
        self.setTextAtts(box, s, c, a, al, an, f)

        box.Draw(o)
        if d:
            self.update()
        self.things.append(box)
        # gc.collect()
        return box

    def handleargs(self, p, t, mn, mx, l, ti, xmin, xmax, w, m, xe, ye):

        if t:
            p.SetTitle(t)
        if mn is not None:
            p.SetMinimum(mn)
        if mx is not None:
            p.SetMaximum(mx)
        if l is not None:
            self.setLineCol(p, l)
        if w is not None:
            p.SetLineWidth(w)
        if m is not None:
            self.setMarker(p, m)
        if ti is not None:
            p.GetXaxis().SetTimeDisplay(1)
            if ti == 1:
                # p.GetXaxis().SetNdivisions(-503)
                # p.GetXaxis().SetTimeFormat("%Y%m%d %H:%M")

                p.GetXaxis().SetTimeFormat("%H:%M")
            p.GetXaxis().SetLabelSize(0.04)
            if ti == 2:
                p.GetXaxis().SetTimeFormat("%Y%m%d")
                p.GetXaxis().SetLabelSize(0.04)
            # p.GetXaxis().SetTimeOffset(0., "gmt")
            p.GetXaxis().SetTimeOffset(0., "local")
        if xmin is not None and xmax is not None:
            p.GetXaxis().SetLimits(xmin, xmax)
        if xe is not None:
            if xe == 0:
                p.GetXaxis().SetNoExponent(True)
            else:
                p.GetXaxis.SetMaxDigits(xe)
        if ye is not None:
            if ye == 0:
                p.GetYaxis().SetNoExponent(True)
            else:
                p.GetYaxis.SetMaxDigits(xe)

    def calcCommonIdx(self, ry, rx=None, rey=None, rex=None, ti=None):

        if rx is None and rey is None and rex is None:  # only y values
            return ry.dropna().index
        if rx is not None and rey is None and rex is None:  # x and y values only
            if ti is None:
                # return ry.dropna().index.intersection(rx.dropna().index)
                return ry.dropna().index
            else:
                return ry.dropna().index
        if rx is None and rey is not None and rex is None:  # y values and errors only
            return ry.dropna().index.intersection(rey.dropna().index)
        if rx is not None and rey is not None and rex is None:  # x values, y values and y errors
            return ry.dropna().index.intersection(rx.dropna().index).intersection(rey.dropna().index)
        if rx is not None and rey is not None and rex is not None:  # x and y values and errors
            return ry.dropna().index.intersection(rx.dropna().index).intersection(rex.dropna().index).intersection(
                rey.dropna().index)

    def rmg2(self, gparams, o='AL', out='', t='', ti=False, mn=None, mx=None, w=None, m=None):

        c = self.c
        c.cd()
        xmin = None
        xmax = None

        graphs = []
        mg = TMultiGraph()
        for params in gparams:
            mg.Add(self.rg(params[0], **params[1]))

        mg.Draw(o)
        self.handleargs(mg, t, mn, mx, None, ti, xmin, xmax, w, m)

        c.Modified()
        c.Update()
        self.things.append(mg)
        if self.isinline() or out:
            self.show(out)
        gc.collect()
        return mg

    def rmg(self, graphs, o='A', out='', t='', ti=False, leg=None, mn=None, mx=None, w=None,
            m=None, n=None, d=True, xmn=None, xmx=None, xe=None, ye=None):

        display = d
        canvas = gPad
        xmin = xmn
        xmax = xmx
        # leg is a list of parameters to pass to TLegend constructor

        mg = TMultiGraph()
        mg.Draw('AL')

        if n is not None:  # use name if given
            mg.SetName(n)
        else:
            if t is not None:  # Else use the title if available
                mg.SetName(t.split(';')[0])

        for graph in graphs:
            gopt = ''
            # if graph.GetL
            if graph.GetMarkerStyle() != 1:
                gopt += 'p'
            # if graph.GetFillStyle() != 1001:
            #    gopt += 'f'
            mg.Add(graph, gopt)

        mg.Draw(o)
        self.handleargs(mg, t, mn, mx, None, ti, xmin, xmax, w, m, xe, ye)

        legend = ''
        if leg is not None:
            # legend = TLegend(*leg)
            legend = TLegend(0.1, 0.7, 0.3, 0.9)
            for graph in graphs:
                lopt = 'l'
                if graph.GetMarkerStyle() != 1:
                    lopt += 'p'
                # if graph.GetFillStyle() != 1001:
                #    lopt += 'f'
                legend.AddEntry(graph, graph.GetTitle(), lopt)
                legend.SetTextSize(0.025)
            legend.Draw()
            self.things.append(legend)

        mg.Draw(o)
        if leg is not None:
            legend.Draw()
        if display:
            canvas.Modified()
            canvas.Update()
            if self.isinline() or out:
                self.show(out)

        #        canvas.Modified()
        #        canvas.Update()
        #        if self.isinline() or out:
        #            self.show(out)
        # gc.collect()
        self.things.append(mg)
        return mg

    def rg4(self, y, x=None, ey=None, ex=None, o='AL', f=False, l='b', out='', t='', ti=None,
            mn=None, mx=None, w=None, d=True, n=None, p=None, m=None, s=None):

        # c = gROOT.GetSelectedPad()
        canvas = gPad  # Get a reference to the current pad
        name = n  # A name for the graph (can be referred to by this in eg TLegend)
        title = t  # Title and axis labels in format 'title;xAxisLabel;yAxisLabel'
        display = d  # True to actually draw the graph
        timeaxis = ti  # 1 for time axis - 2 for date axis
        options = o  # Options for drawing the graph - see https://root.cern.ch/doc/master/classTGraphPainter.html
        linecolour = l  # Colour to set the line for this Graph - see setDefStyle above for definitions
        fit = f  # In format 'function:options' were function is a function name or reference
        #     For options see...
        #      https://root.cern.ch/doc/master/classTGraph.html#aa978c8ee0162e661eae795f6f3a35589
        #      will prob enhance this to be [func, fitoptions, graphoptions, xmin, xmaax]
        step = s  # Draw graph with steps
        # c.cd()
        xmin = None
        xmax = None

        # TODO  do not reindex if no nans
        # Drop NaNs and then calculate a common index
        commonIdx = self.calcCommonIdx(y, x, ey, ex, ti=timeaxis)
        cy = y.reindex(commonIdx)
        cx = x
        if x is not None:
            if timeaxis is None:
                cx = x.reindex(commonIdx)
        if ey is not None:
            cey = ey.reindex(commonIdx)
        if ex is not None:
            cex = ex.reindex(commonIdx)

        # Convert x axis if
        if timeaxis is not None:
            cx = cx.astype(np.int64) / 1000000000.

        # print('len cx %s len cy %s' % (len(cx), len(cy)))

        # if step requested and no errors and no markers - make a stepped line
        if ex is not None or ey is not None or m is not None:
            step = None
        if step is not None:
            cy = cy.repeat(2)[:-1]
            if x is not None:
                cx = cx.repeat(2)[1:]

        # Decide whether to create TGraph or TGraphErrors and populate
        if ex is None and ey is None:  # No errors
            if x is None:
                cx = array('d', [float(i + 1) for i in range(len(cy))])
            else:
                if timeaxis is not None:
                    cx = array('d', [cx[i] for i in range(len(cx))])
                else:
                    cx = array('d', [float(cx[i]) for i in range(len(cx))])
                    # cx = cx.values
            g = TGraph(len(cy), cx, cy.values)
        else:
            if x is None:
                cx = array('d', [cy.index[i] for i in range(len(cy))])
            else:
                if timeaxis is not None:
                    cx = array('d', [cx[i] for i in range(len(cy))])
                else:
                    cx = cx.values
            if ex is None:
                cex = array('d', [0.0 for i in range(len(cy))])
            else:
                cex = cex.values
            if ey is None:
                cey = array('d', [0.0 for i in range(len(cy))])
            else:
                cey = cey.values
            g = TGraphErrors(len(cy), cx, cy.values, cex, cey)

        if name is not None:  # use name if given
            g.SetName(name)
        else:
            if title is not None:  # Else use the title if available
                g.SetName(title.split(';')[0])

        g.Draw(options)  # Call Draw to generate the axes
        self.handleargs(g, title, mn, mx, linecolour, timeaxis, xmin, xmax, w, m)

        if fit:
            func = fit
            fitoptions = ''
            if ':' in fit:
                func, fitoptions = fit.split(':')
            g.Draw(options)  # Is this needed if got one above ?
            fitpointer = g.Fit(func, fitoptions)

        g.Draw(options)  # again !
        if display:
            canvas.Modified()
            canvas.Update()
            if self.isinline() or out:
                self.show(out)
        self.things.append(g)
        # gc.collect()
        if fit:
            return g, fitpointer
        return g

    def checkTypesNans(self, x, y, ex, ey, ti=None):
        gotnans = False
        allseries = True
        anyseriesnans = False
        anynumpynans = False

        arrs = [x, y, ex, ey]
        if len(y) == 0:
            return x, y, ex, ey
        for i in range(len(arrs)):
            if isinstance(arrs[i], list):  # If a list convert to an array of doubles
                arrs[i] = np.asarray(arrs[i], dtype='float64')
            if arrs[i] is not None:  # Check pd.Series and np.ndarray for nans
                if isinstance(arrs[i], pd.core.series.Series):
                    anyseriesnans = anyseriesnans or arrs[i].isnull().values.any()
                if isinstance(arrs[i], np.ndarray):
                    allseries = False
                    anynumpynans = anynumpynans or np.isnan(arrs[i]).any()

        if allseries:  # If all are series remove nans and reindex
            if anyseriesnans:
                pass
                commonIdx = self.calcCommonIndex(x, y, ex, ey, ti)
                arrs[1] = arrs[1].reindex(commonIdx)
                if arrs[0] is not None:
                    if ti is None:
                        arrs[0] = arrs[0].reindex(commonIdx)
                if arrs[4] is not None:
                    arrs[4] = arrs[4].reindex(commonIdx)
                if arrs[3] is not None:
                    arrs[3] = arrs[3].reindex(commonIdx)
        else:
            if anyseriesnans or anynumpynans:
                gotnans = True
        if gotnans:  # Bomb out if got nans and not all series
            return x, y, ex, ey, True

        for i in range(len(arrs)):
            if arrs[i] is not None:
                if isinstance(arrs[i], pd.core.series.Series) or \
                        isinstance(arrs[i],
                                   pd.core.indexes.datetimes.DatetimeIndex):  # Exract np.ndarray from pd.Series
                    arrs[i] = arrs[i].values
                if not isinstance(arrs[i], np.ndarray):  # Convert to np.ndarray if not already
                    arrs[i] = np.asarray(arrs[i], dtype='float64')
                if arrs[i].dtype != np.float64:  # Convert to double if not already
                    arrs[i] = arrs[i].astype('float64')
                if not arrs[i].flags['C_CONTIGUOUS']:  # Make contiguous if not already
                    arrs[i] = np.ascontiguousarray(arrs[i])
        return arrs[0], arrs[1], arrs[2], arrs[3], False

    def rg(self, y, x=None, ey=None, ex=None, o='AL', f=False, l='b', out='', t='', ti=None,
           mn=None, mx=None, w=None, d=True, n=None, p=None, m=None, s=None, xe=None, ye=None):

        # c = gROOT.GetSelectedPad()
        canvas = gPad  # Get a reference to the current pad
        name = n  # A name for the graph (can be referred to by this in eg TLegend)
        title = t  # Title and axis labels in format 'title;xAxisLabel;yAxisLabel'
        display = d  # True to actually draw the graph
        timeaxis = ti  # 1 for time axis - 2 for date axis
        options = o  # Options for drawing the graph - see https://root.cern.ch/doc/master/classTGraphPainter.html
        linecolour = l  # Colour to set the line for this Graph - see setDefStyle above for definitions
        fit = f  # In format 'function:options' were function is a function name or reference
        #     For options see...
        #      https://root.cern.ch/doc/master/classTGraph.html#aa978c8ee0162e661eae795f6f3a35589
        #      will prob enhance this to be [func, fitoptions, graphoptions, xmin, xmaax]
        step = s  # Draw graph with steps
        # c.cd()
        xmin = None
        xmax = None

        # Transform input data
        gotnans = False
        if y is not None:
            x, y, ex, ey, gotnans = self.checkTypesNans(x, y, ex, ey)
        if gotnans:
            print('You got NaNs')
            return None

        if timeaxis is not None and x is not None:
            x = x / 1000000000.

        # if step requested and no errors and no markers - make a stepped line
        if ex is not None or ey is not None or m is not None:
            step = None
        if step is not None:
            y = y.repeat(2)[:-1]
            if x is not None:
                x = x.repeat(2)[1:]

        # Decide whether to create TGraph or TGraphErrors and populate
        if y is None:
            g = TGraph()
        else:
            if x is None:
                x = np.arange(1, len(y) + 1, dtype=str(y.dtype))
            if ex is None and ey is None:  # No errors
                g = TGraph(len(y), x, y)
            else:
                if ex is None:
                    ex = np.zeros(len(y), dtype=str(y.dtype))
                if ey is None:
                    ey = np.zeros(len(y), dtype=str(y.dtype))
                g = TGraphErrors(len(y), x, y, ex, ey)

        if name is not None:  # use name if given
            g.SetName(name)
        else:
            if title is not None:  # Else use the title if available
                g.SetName(title.split(';')[0])

        g.Draw(options)  # Call Draw to generate the axes
        self.handleargs(g, title, mn, mx, linecolour, timeaxis, xmin, xmax, w, m, xe, ye)

        if fit:
            func = fit
            fitoptions = ''
            if ':' in fit:
                func, fitoptions = fit.split(':')
            g.Draw(options)  # Is this needed if got one above ?
            fitpointer = g.Fit(func, fitoptions)

        g.Draw(options)  # again !
        img = None
        if display:
            canvas.Modified()
            canvas.Update()
            if self.isinline() or out:
                img = self.show(out)
        self.things.append(g)
        # gc.collect()
        if fit:
            return g, fitpointer
        if img is not None:
            return g, img
        return g

    def rh(self, x, nbins=40, xlow=None, xhigh=None, o='', t='h', f=False,
           n=None, l='W', out='', ti=None, mn=None, mx=None, xmin=None, xmax=None,
           fi=None, w=None, d=True, m=None, p=None, xe=None, ye=None):

        c = self.c
        c.cd()

        xt = 0.
        if xlow is None and xhigh is None:
            xt = (x.max() - x.min()) * 0.05
        if xlow is None:
            xlow = x.min() - xt
        if xhigh is None:
            xhigh = x.max() + xt
        if p is not None:
            xlow, xhigh = x.quantile([p, 1 - p])

#        if ti:
#            nbins = len(x.index)
#            bindiff = x.index[1] - x.index[0]
#            xlow = (x.index[0] - bindiff).value / 1000000000.0
#            xhigh = (x.index[-1].value / 1000000000.0) + 1.
#            print('xs ', xlow, xhigh)
        h = TH1D(t, t, nbins, xlow, xhigh)
        self.handleargs(h, t, mn, mx, l, ti, xmin, xmax, w, m, xe, ye)
        gotwgts = False
        if len(x.shape) == 1:
        # if 1 == 1:
            # xd = x.replace([np.inf, -np.inf], np.nan).dropna()
            if ti:
                x = x.values.astype(int) / 1000000000
                h.FillN(len(x), x, nullptr)
                # [h.fill(x.index[i].value / 1000000000.0, x[i]) for i in range(len(d.index))]
            else:
                h.FillN(len(x), x, nullptr)
                # [h.Fill(go) for go in xd]
        else:
            gotwgts = True
            xd = x.replace([np.inf, -np.inf], np.nan).dropna()
            # h.Sumw2()
            h.FillN(len(xd), xd.iloc[:,0].values, xd.iloc[:,1].values)
            #[h.Fill(go, xd.iat[i, 1]) for i, go in enumerate(xd.iloc[:0])]
            if len(x.shape) == 2:
                xaxis = h.GetXaxis()
                for i, val in enumerate(xd.iloc[:,2]):
                    xaxis.SetBinLabel(i+1, str(val))
                h.GetXaxis().SetLabelSize(0.09)
        self.st.SetOptStat(110010)
        if fi is not None:
            h.SetFillColor(fi)
            # self.setFillCol(h, fi)
        if n is not None:
            h.Scale(float(n) / h.Integral(), 'width')
        fitpointer = None
        if f:
            func = f
            fitoptions = ''
            if ':' in f:
                func, fitoptions = f.split(':')
            self.st.SetOptFit(1111)
            if f == 'gaus':
                self.st.SetOptStat(220110010)
            fitpointer = h.Fit(func, fitoptions)
        else:
            self.st.SetOptFit(0)
        h.Draw(o)
        if gotwgts:
            h.Draw('histsame')
        img = None
        if d:
            c.Modified()
            c.Update()
            if self.isinline() or out:
                img = self.show(out)
                # js = TBufferJSON.ExportToFile('hist.json', h)  # .Data()
                # with open('hist.json', 'w') as fh:
                #     fh.write(js)
                # img = js
        self.things.append(h)
        if fitpointer is not None:
            self.things.append(fitpointer)
        gc.collect()
        if f:
            return h, fitpointer
        if img is not None:
            return h, img
        return h

    def rh2(self, x1, y1, nbinsx=40, nbinsy=40, xlow=None, xhigh=None, ylow=None, yhigh=None,
            o='', t='h', out='', l='b', wgts=None, d=True, p=None, fi=None):

        c = self.c
        c.cd()

        if p is not None:
            self.st.SetPalette(p)

        # commonIdx = x1.dropna().index.intersection(y1.dropna().index)
        # x = x1.reindex(commonIdx).values
        # y = y1.reindex(commonIdx).values
        x = x1
        y = y1

        # xt = (x.max() - x.min) * 0.05
        # yt = (y.max() - y.min) * 0.05

        h = TH2D(t, t, nbinsx, xlow, xhigh, nbinsy, ylow, yhigh)

        if wgts is None:
            h.FillN(len(x), x, y, nullptr)
            # [h.Fill(x[i], y[i]) for i in range(len(x))]
        else:
            h.Sumw2()
            h.FillN(len(x), x, y, wgts)
            # [h.Fill(x[i], y[i], wgts[i]) for i in range(len(x))]

        self.setLineCol(h, l)
        if fi is not None:
            h.SetFillColor(fi)
        h.Draw(o)
        img = None
        if d:
            c.Modified()
            c.Update()
            if self.isinline() or out:
                img = self.show(out)
        self.things.append(h)
        gc.collect()
        # self.st.SetPalette(self.defaultPalette)
        if img is not None:
            return h, img
        return h

    def rp(self, x1, y1, nbinsx=40, nbinsy=40, xlow=None, xhigh=None, ylow=None, yhigh=None,
           out='', o='', t='h', l='b', fi=None):

        c = self.c
        c.cd()
        commonIdx = x1.dropna().index.intersection(y1.dropna().index)
        x = x1.reindex(commonIdx).values
        y = y1.reindex(commonIdx).values

        xt = (x.max() - x.min()) * 0.05
        yt = (y.max() - y.min()) * 0.05
        if not xlow: xlow = x1.min() - xt
        if not xhigh: xhigh = x1.max() + xt
        if not ylow: ylow = y1.min() - yt
        if not yhigh: yhigh = y1.max() + yt
        h = TProfile(t, t, nbinsx, xlow, xhigh)
        [h.Fill(x[i], y[i]) for i in range(len(x))]

        self.setLineCol(h, l)
        h.Draw(o)
        c.Modified()
        c.Update()
        self.things.append(h)
        if self.isinline() or out: self.show(out)
        gc.collect()
        return h


if __name__ == '__main__':
    pr = pROOT(True)
    c1 = pr.createCanvas('c1')
    # import sys
    # sys.path.append('\\\\nas.lon.algo\\docs/Emmett/repos/static/scripts')
    # dh = DataHelper(False)
    # df = dh.getLUT('Trades', 'data', asframe = True)
    # s1, s2 = 'VOW3d VOWd'.split()
    # df1 = df[df.symbol == s1]
    # df2 = df[df.symbol == s2]
    # pr.rg(df2.amount.astype(float).cumsum(), x = df.time, ti = 1, o = 'AL*')
    # pr.rmg([[df1.amount.astype(float).cumsum(), {'x': df1.time, 'o': 'A'}],
    #        [df2.amount.astype(float).cumsum(), {'x': df2.time, 'o': 'A', 'l': 'r'}]],
    #       ti = 1, o = 'AL*', t = '%s - %s trades;time;quantity' % (s1, s2))

/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.shell;

import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.SwingUtilities;

import com.github.rjeschke.txtmark.Processor;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.ptoss.util.Throwables;
import com.palantir.util.Pair;

final class AtlasShellHelp {
    void show() {
        show(getTitlesAndHtmlContentses());
    }

    private void show(List<Pair<String,String>> titlesAndHtmlContentses) {
        JTabbedPane jTabbedPane = new JTabbedPane();
        for (Pair<String, String> titleAndHtmlContents : titlesAndHtmlContentses) {
            String title = titleAndHtmlContents.lhSide;
            String htmlContents = titleAndHtmlContents.rhSide;
            JEditorPane jEditorPane = new JEditorPane("text/html", htmlContents);
            jEditorPane.setEditable(false);
            final JScrollPane jScrollPane = new JScrollPane(jEditorPane);
            SwingUtilities.invokeLater(PTExecutors.wrap(new Runnable() {
                @Override
                public void run() {
                    jScrollPane.getVerticalScrollBar().setValue(0);
                }
             }));
            jTabbedPane.addTab(title, jScrollPane);
        }
        JFrame jFrame = new JFrame("AtlasDB Shell Help");
        jFrame.setLocation(200, 200);
        jFrame.setSize(800, 800);
        jFrame.add(jTabbedPane);
        jFrame.setVisible(true);
    }

    private List<Pair<String, String>> getTitlesAndHtmlContentses() {
        Iterable<String> titles = getTitles(read("/help/_toc.txt"));
        List<Pair<String, String>> titlesAndHtmlContentses = Lists.newArrayList();
        for (String title : titles) {
            String htmlContents = getHtmlContents(title);
            Pair<String, String> titleAndHtmlContents = Pair.create(title, htmlContents);
            titlesAndHtmlContentses.add(titleAndHtmlContents);
        }
        return titlesAndHtmlContentses;
    }

    private String getHtmlContents(String title) {
        String markdownContents = read("/help/" + title + ".txt");
        return Processor.process(markdownContents);
    }

    private Iterable<String> getTitles(String toc) {
        Matcher matcher = Pattern.compile(":(\\w+)").matcher(toc);
        List<String> titles = Lists.newArrayList();
        while (matcher.find()) {
            titles.add(matcher.group(1));
        }
        return titles;
    }

    private String read(String path) {
        try {
            URL url = this.getClass().getResource(path);
            Charset charSet = Charset.forName("UTF-8");
            Reader reader = Resources.asCharSource(url, charSet).openStream();
            return CharStreams.toString(reader);
        } catch (IOException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }
}

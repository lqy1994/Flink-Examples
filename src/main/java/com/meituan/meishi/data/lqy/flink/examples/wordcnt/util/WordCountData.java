/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.meituan.meishi.data.lqy.flink.examples.wordcnt.util;

/**
 * Provides the default data sets used for the WordCount example program.
 * The default data sets are used, if no parameters are given to the program.
 *
 */
public class WordCountData {

	public static final String[] WORDS = new String[] {
		"To be, or not to be,--that is the question:--",
		"Whether 'tis nobler in the mind to suffer",
		"The slings and arrows of outrageous fortune",
		"Or to take arms against a sea of troubles,",
		"And by opposing end them?--To die,--to sleep,--",
		"No more; and by a sleep to say we end",
		"The heartache, and the thousand natural shocks",
		"That flesh is heir to,--'tis a consummation",
		"Devoutly to be wish'd. To die,--to sleep;--",
		"To sleep! perchance to dream:--ay, there's the rub;",
		"For in that sleep of death what dreams may come,",
		"When we have shuffled off this mortal coil,",
		"Must give us pause: there's the respect",
		"That makes calamity of so long life;",
		"For who would bear the whips and scorns of time,",
		"The oppressor's wrong, the proud man's contumely,",
		"The pangs of despis'd love, the law's delay,",
		"The insolence of office, and the spurns",
		"That patient merit of the unworthy takes,",
		"When he himself might his quietus make",
		"With a bare bodkin? who would these fardels bear,",
		"To grunt and sweat under a weary life,",
		"But that the dread of something after death,--",
		"The undiscover'd country, from whose bourn",
		"No traveller returns,--puzzles the will,",
		"And makes us rather bear those ills we have",
		"Than fly to others that we know not of?",
		"Thus conscience does make cowards of us all;",
		"And thus the native hue of resolution",
		"Is sicklied o'er with the pale cast of thought;",
		"And enterprises of great pith and moment,",
		"With this regard, their currents turn awry,",
		"And lose the name of action.--Soft you now!",
		"The fair Ophelia!--Nymph, in thy orisons",
		"Be all my sins remember'd."
	};

	public static final String TEXT = "Goethe - Faust: Der Tragoedie erster Teil\n" + "Prolog im Himmel.\n"
			+ "Der Herr. Die himmlischen Heerscharen. Nachher Mephistopheles. Die drei\n" + "Erzengel treten vor.\n"
			+ "RAPHAEL: Die Sonne toent, nach alter Weise, In Brudersphaeren Wettgesang,\n"
			+ "Und ihre vorgeschriebne Reise Vollendet sie mit Donnergang. Ihr Anblick\n"
			+ "gibt den Engeln Staerke, Wenn keiner Sie ergruenden mag; die unbegreiflich\n"
			+ "hohen Werke Sind herrlich wie am ersten Tag.\n"
			+ "GABRIEL: Und schnell und unbegreiflich schnelle Dreht sich umher der Erde\n"
			+ "Pracht; Es wechselt Paradieseshelle Mit tiefer, schauervoller Nacht. Es\n"
			+ "schaeumt das Meer in breiten Fluessen Am tiefen Grund der Felsen auf, Und\n"
			+ "Fels und Meer wird fortgerissen Im ewig schnellem Sphaerenlauf.\n"
			+ "MICHAEL: Und Stuerme brausen um die Wette Vom Meer aufs Land, vom Land\n"
			+ "aufs Meer, und bilden wuetend eine Kette Der tiefsten Wirkung rings umher.\n"
			+ "Da flammt ein blitzendes Verheeren Dem Pfade vor des Donnerschlags. Doch\n"
			+ "deine Boten, Herr, verehren Das sanfte Wandeln deines Tags.\n"
			+ "ZU DREI: Der Anblick gibt den Engeln Staerke, Da keiner dich ergruenden\n"
			+ "mag, Und alle deine hohen Werke Sind herrlich wie am ersten Tag.\n"
			+ "MEPHISTOPHELES: Da du, o Herr, dich einmal wieder nahst Und fragst, wie\n"
			+ "alles sich bei uns befinde, Und du mich sonst gewoehnlich gerne sahst, So\n"
			+ "siehst du mich auch unter dem Gesinde. Verzeih, ich kann nicht hohe Worte\n"
			+ "machen, Und wenn mich auch der ganze Kreis verhoehnt; Mein Pathos braechte\n"
			+ "dich gewiss zum Lachen, Haettst du dir nicht das Lachen abgewoehnt. Von\n"
			+ "Sonn' und Welten weiss ich nichts zu sagen, Ich sehe nur, wie sich die\n"
			+ "Menschen plagen. Der kleine Gott der Welt bleibt stets von gleichem\n"
			+ "Schlag, Und ist so wunderlich als wie am ersten Tag. Ein wenig besser\n"
			+ "wuerd er leben, Haettst du ihm nicht den Schein des Himmelslichts gegeben;\n"
			+ "Er nennt's Vernunft und braucht's allein, Nur tierischer als jedes Tier\n"
			+ "zu sein. Er scheint mir, mit Verlaub von euer Gnaden, Wie eine der\n"
			+ "langbeinigen Zikaden, Die immer fliegt und fliegend springt Und gleich im\n"
			+ "Gras ihr altes Liedchen singt; Und laeg er nur noch immer in dem Grase! In\n"
			+ "jeden Quark begraebt er seine Nase.\n"
			+ "DER HERR: Hast du mir weiter nichts zu sagen? Kommst du nur immer\n"
			+ "anzuklagen? Ist auf der Erde ewig dir nichts recht?\n"
			+ "MEPHISTOPHELES: Nein Herr! ich find es dort, wie immer, herzlich\n"
			+ "schlecht. Die Menschen dauern mich in ihren Jammertagen, Ich mag sogar\n"
			+ "die armen selbst nicht plagen.\n" + "DER HERR: Kennst du den Faust?\n" + "MEPHISTOPHELES: Den Doktor?\n"
			+ "DER HERR: Meinen Knecht!\n"
			+ "MEPHISTOPHELES: Fuerwahr! er dient Euch auf besondre Weise. Nicht irdisch\n"
			+ "ist des Toren Trank noch Speise. Ihn treibt die Gaerung in die Ferne, Er\n"
			+ "ist sich seiner Tollheit halb bewusst; Vom Himmel fordert er die schoensten\n"
			+ "Sterne Und von der Erde jede hoechste Lust, Und alle Naeh und alle Ferne\n"
			+ "Befriedigt nicht die tiefbewegte Brust.\n"
			+ "DER HERR: Wenn er mir auch nur verworren dient, So werd ich ihn bald in\n"
			+ "die Klarheit fuehren. Weiss doch der Gaertner, wenn das Baeumchen gruent, Das\n"
			+ "Bluet und Frucht die kuenft'gen Jahre zieren.\n"
			+ "MEPHISTOPHELES: Was wettet Ihr? den sollt Ihr noch verlieren! Wenn Ihr\n"
			+ "mir die Erlaubnis gebt, Ihn meine Strasse sacht zu fuehren.\n"
			+ "DER HERR: Solang er auf der Erde lebt, So lange sei dir's nicht verboten,\n"
			+ "Es irrt der Mensch so lang er strebt.\n"
			+ "MEPHISTOPHELES: Da dank ich Euch; denn mit den Toten Hab ich mich niemals\n"
			+ "gern befangen. Am meisten lieb ich mir die vollen, frischen Wangen. Fuer\n"
			+ "einem Leichnam bin ich nicht zu Haus; Mir geht es wie der Katze mit der Maus.\n"
			+ "DER HERR: Nun gut, es sei dir ueberlassen! Zieh diesen Geist von seinem\n"
			+ "Urquell ab, Und fuehr ihn, kannst du ihn erfassen, Auf deinem Wege mit\n"
			+ "herab, Und steh beschaemt, wenn du bekennen musst: Ein guter Mensch, in\n"
			+ "seinem dunklen Drange, Ist sich des rechten Weges wohl bewusst.\n"
			+ "MEPHISTOPHELES: Schon gut! nur dauert es nicht lange. Mir ist fuer meine\n"
			+ "Wette gar nicht bange. Wenn ich zu meinem Zweck gelange, Erlaubt Ihr mir\n"
			+ "Triumph aus voller Brust. Staub soll er fressen, und mit Lust, Wie meine\n"
			+ "Muhme, die beruehmte Schlange.\n"
			+ "DER HERR: Du darfst auch da nur frei erscheinen; Ich habe deinesgleichen\n"
			+ "nie gehasst. Von allen Geistern, die verneinen, ist mir der Schalk am\n"
			+ "wenigsten zur Last. Des Menschen Taetigkeit kann allzu leicht erschlaffen,\n"
			+ "er liebt sich bald die unbedingte Ruh; Drum geb ich gern ihm den Gesellen\n"
			+ "zu, Der reizt und wirkt und muss als Teufel schaffen. Doch ihr, die echten\n"
			+ "Goettersoehne, Erfreut euch der lebendig reichen Schoene! Das Werdende, das\n"
			+ "ewig wirkt und lebt, Umfass euch mit der Liebe holden Schranken, Und was\n"
			+ "in schwankender Erscheinung schwebt, Befestigt mit dauernden Gedanken!\n"
			+ "(Der Himmel schliesst, die Erzengel verteilen sich.)\n"
			+ "MEPHISTOPHELES (allein): Von Zeit zu Zeit seh ich den Alten gern, Und\n"
			+ "huete mich, mit ihm zu brechen. Es ist gar huebsch von einem grossen Herrn,\n"
			+ "So menschlich mit dem Teufel selbst zu sprechen.";

	public static final String COUNTS = "machen 1\n" + "zeit 2\n" + "heerscharen 1\n" + "keiner 2\n" + "meine 3\n"
			+ "fuehr 1\n" + "triumph 1\n" + "kommst 1\n" + "frei 1\n" + "schaffen 1\n" + "gesinde 1\n"
			+ "langbeinigen 1\n" + "schalk 1\n" + "besser 1\n" + "solang 1\n" + "meer 4\n" + "fragst 1\n"
			+ "gabriel 1\n" + "selbst 2\n" + "bin 1\n" + "sich 7\n" + "du 11\n" + "sogar 1\n" + "geht 1\n"
			+ "immer 4\n" + "mensch 2\n" + "befestigt 1\n" + "lebt 2\n" + "mag 3\n" + "engeln 2\n" + "breiten 1\n"
			+ "blitzendes 1\n" + "tags 1\n" + "sie 2\n" + "plagen 2\n" + "allzu 1\n" + "meisten 1\n" + "o 1\n"
			+ "pfade 1\n" + "kennst 1\n" + "nichts 3\n" + "gedanken 1\n" + "befriedigt 1\n" + "mich 6\n" + "s 3\n"
			+ "es 8\n" + "verneinen 1\n" + "er 13\n" + "gleich 1\n" + "baeumchen 1\n" + "donnergang 1\n"
			+ "wunderlich 1\n" + "reise 1\n" + "urquell 1\n" + "doch 3\n" + "aufs 2\n" + "toten 1\n" + "niemals 1\n"
			+ "eine 2\n" + "hab 1\n" + "darfst 1\n" + "da 5\n" + "gen 1\n" + "einem 2\n" + "teil 1\n" + "das 7\n"
			+ "speise 1\n" + "wenig 1\n" + "sterne 1\n" + "geb 1\n" + "welten 1\n" + "alle 3\n" + "toent 1\n"
			+ "gras 1\n" + "felsen 1\n" + "kette 1\n" + "ich 14\n" + "fuer 2\n" + "als 3\n" + "mein 1\n"
			+ "schoene 1\n" + "verzeih 1\n" + "schwankender 1\n" + "wie 9\n" + "menschlich 1\n" + "gaertner 1\n"
			+ "taetigkeit 1\n" + "bange 1\n" + "liebe 1\n" + "sei 2\n" + "seh 1\n" + "tollheit 1\n" + "am 6\n"
			+ "michael 1\n" + "geist 1\n" + "ab 1\n" + "nahst 1\n" + "vollendet 1\n" + "liebt 1\n" + "brausen 1\n"
			+ "nase 1\n" + "erlaubt 1\n" + "weiss 2\n" + "schnellem 1\n" + "deinem 1\n" + "gleichem 1\n"
			+ "gaerung 1\n" + "dauernden 1\n" + "deines 1\n" + "vorgeschriebne 1\n" + "irdisch 1\n" + "worte 1\n"
			+ "verehren 1\n" + "hohen 2\n" + "weise 2\n" + "kuenft 1\n" + "werdende 1\n" + "wette 2\n" + "wuetend 1\n"
			+ "erscheinung 1\n" + "gar 2\n" + "verlieren 1\n" + "braucht 1\n" + "weiter 1\n" + "trank 1\n"
			+ "tierischer 1\n" + "wohl 1\n" + "verteilen 1\n" + "verhoehnt 1\n" + "schaeumt 1\n" + "himmelslichts 1\n"
			+ "unbedingte 1\n" + "herzlich 1\n" + "anblick 2\n" + "nennt 1\n" + "gruent 1\n" + "bluet 1\n"
			+ "leichnam 1\n" + "erschlaffen 1\n" + "jammertagen 1\n" + "zieh 1\n" + "ihm 3\n" + "besondre 1\n"
			+ "ihn 5\n" + "grossen 1\n" + "vollen 1\n" + "ihr 7\n" + "boten 1\n" + "voller 1\n" + "singt 1\n"
			+ "muhme 1\n" + "schon 1\n" + "last 1\n" + "kleine 1\n" + "paradieseshelle 1\n" + "nein 1\n" + "echten 1\n"
			+ "unter 1\n" + "bei 1\n" + "herr 11\n" + "gern 3\n" + "sphaerenlauf 1\n" + "stets 1\n" + "ganze 1\n"
			+ "braechte 1\n" + "fordert 1\n" + "schoensten 1\n" + "herrlich 2\n" + "gegeben 1\n" + "allein 2\n"
			+ "reichen 1\n" + "schauervoller 1\n" + "musst 1\n" + "recht 1\n" + "bleibt 1\n" + "pracht 1\n"
			+ "treibt 1\n" + "befangen 1\n" + "was 2\n" + "menschen 3\n" + "jede 1\n" + "hohe 1\n" + "tiefsten 1\n"
			+ "bilden 1\n" + "drum 1\n" + "gibt 2\n" + "guter 1\n" + "fuerwahr 1\n" + "im 3\n" + "grund 1\n" + "in 9\n"
			+ "hoechste 1\n" + "schliesst 1\n" + "fels 1\n" + "steh 1\n" + "euer 1\n" + "erster 1\n" + "ersten 3\n"
			+ "goettersoehne 1\n" + "brechen 1\n" + "tiefen 1\n" + "frucht 1\n" + "kreis 1\n" + "siehst 1\n"
			+ "wege 1\n" + "ist 8\n" + "zikaden 1\n" + "frischen 1\n" + "ruh 1\n" + "deine 2\n" + "maus 1\n"
			+ "brudersphaeren 1\n" + "nachher 1\n" + "euch 4\n" + "gnaden 1\n" + "anzuklagen 1\n" + "schlange 1\n"
			+ "staerke 2\n" + "erde 4\n" + "verlaub 1\n" + "sanfte 1\n" + "holden 1\n" + "sonst 1\n" + "treten 1\n"
			+ "sahst 1\n" + "alten 1\n" + "um 1\n" + "wieder 1\n" + "alter 1\n" + "altes 1\n" + "nun 1\n" + "lieb 1\n"
			+ "gesellen 1\n" + "erscheinen 1\n" + "wirkt 2\n" + "haettst 2\n" + "nur 7\n" + "tiefbewegte 1\n"
			+ "lachen 2\n" + "drange 1\n" + "schlag 1\n" + "schein 1\n" + "muss 1\n" + "verworren 1\n" + "weges 1\n"
			+ "allen 1\n" + "gewoehnlich 1\n" + "alles 1\n" + "halb 1\n" + "stuerme 1\n" + "springt 1\n" + "sollt 1\n"
			+ "klarheit 1\n" + "so 6\n" + "erfassen 1\n" + "liedchen 1\n" + "prolog 1\n" + "zur 1\n" + "fressen 1\n"
			+ "zum 1\n" + "faust 2\n" + "erzengel 2\n" + "jahre 1\n" + "sonn 1\n" + "raphael 1\n" + "land 2\n"
			+ "lang 1\n" + "gelange 1\n" + "lust 2\n" + "welt 1\n" + "sehe 1\n" + "ihre 1\n" + "jedes 1\n"
			+ "erfreut 1\n" + "seiner 1\n" + "denn 1\n" + "wandeln 1\n" + "wechselt 1\n" + "jeden 1\n" + "dort 1\n"
			+ "schlecht 1\n" + "wenigsten 1\n" + "wuerd 1\n" + "schranken 1\n" + "bewusst 2\n" + "seinem 2\n"
			+ "gehasst 1\n" + "sein 1\n" + "meinem 1\n" + "meinen 1\n" + "pathos 1\n" + "herrn 1\n" + "lange 2\n"
			+ "herab 1\n" + "diesen 1\n" + "ihren 1\n" + "beruehmte 1\n" + "goethe 1\n" + "tag 3\n" + "tier 1\n"
			+ "quark 1\n" + "dank 1\n" + "seine 1\n" + "teufel 2\n" + "zweck 1\n" + "wenn 7\n" + "soll 1\n"
			+ "wirkung 1\n" + "erlaubnis 1\n" + "lebendig 1\n" + "uns 1\n" + "leicht 1\n" + "gewiss 1\n"
			+ "schnell 1\n" + "und 29\n" + "gerne 1\n" + "rechten 1\n" + "umher 2\n" + "vernunft 1\n" + "grase 1\n"
			+ "nach 1\n" + "leben 1\n" + "gott 1\n" + "der 29\n" + "des 5\n" + "doktor 1\n" + "beschaemt 1\n"
			+ "dreht 1\n" + "habe 1\n" + "sagen 2\n" + "bekennen 1\n" + "dunklen 1\n" + "wettet 1\n" + "den 9\n"
			+ "mephistopheles 9\n" + "dem 4\n" + "auch 4\n" + "kann 2\n" + "armen 1\n" + "mir 9\n" + "strebt 1\n"
			+ "gut 2\n" + "mit 11\n" + "bald 2\n" + "himmlischen 1\n" + "himmel 3\n" + "noch 3\n" + "kannst 1\n"
			+ "deinesgleichen 1\n" + "flammt 1\n" + "ergruenden 2\n" + "nacht 1\n" + "scheint 1\n" + "ferne 2\n"
			+ "tragoedie 1\n" + "abgewoehnt 1\n" + "reizt 1\n" + "geistern 1\n" + "nicht 10\n" + "sacht 1\n"
			+ "unbegreiflich 2\n" + "schnelle 1\n" + "einmal 1\n" + "werd 1\n" + "werke 2\n" + "begraebt 1\n"
			+ "knecht 1\n" + "rings 1\n" + "wird 1\n" + "katze 1\n" + "huete 1\n" + "fortgerissen 1\n" + "gebt 1\n"
			+ "huebsch 1\n" + "hast 1\n" + "irrt 1\n" + "befinde 1\n" + "sind 2\n" + "fuehren 2\n" + "fliegt 1\n"
			+ "ewig 3\n" + "brust 2\n" + "sonne 1\n" + "sprechen 1\n" + "ein 3\n" + "strasse 1\n" + "von 8\n"
			+ "ueberlassen 1\n" + "dir 4\n" + "vom 3\n" + "zu 11\n" + "schwebt 1\n" + "die 22\n" + "vor 2\n"
			+ "wangen 1\n" + "wettgesang 1\n" + "donnerschlags 1\n" + "find 1\n" + "dich 3\n" + "umfass 1\n"
			+ "verboten 1\n" + "laeg 1\n" + "nie 1\n" + "drei 2\n" + "dauern 1\n" + "toren 1\n" + "dauert 1\n"
			+ "verheeren 1\n" + "fliegend 1\n" + "aus 1\n" + "staub 1\n" + "fluessen 1\n" + "haus 1\n" + "auf 5\n"
			+ "dient 2\n" + "tiefer 1\n" + "naeh 1\n" + "zieren 1\n";

	public static final String STREAMING_COUNTS_AS_TUPLES = "(machen,1)\n" + "(zeit,1)\n" + "(zeit,2)\n" + "(heerscharen,1)\n" + "(keiner,1)\n"
			+ "(keiner,2)\n" + "(meine,1)\n" + "(meine,2)\n" + "(meine,3)\n" + "(fuehr,1)\n" + "(triumph,1)\n" + "(kommst,1)\n"
			+ "(frei,1)\n" + "(schaffen,1)\n" + "(gesinde,1)\n" + "(langbeinigen,1)\n" + "(schalk,1)\n" + "(besser,1)\n" + "(solang,1)\n"
			+ "(meer,1)\n" + "(meer,2)\n" + "(meer,3)\n" + "(meer,4)\n" + "(fragst,1)\n" + "(gabriel,1)\n" + "(selbst,1)\n" + "(selbst,2)\n"
			+ "(bin,1)\n" + "(sich,1)\n" + "(sich,2)\n" + "(sich,3)\n" + "(sich,4)\n" + "(sich,5)\n" + "(sich,6)\n" + "(sich,7)\n" + "(du,1)\n"
			+ "(du,2)\n" + "(du,3)\n" + "(du,4)\n" + "(du,5)\n" + "(du,6)\n" + "(du,7)\n" + "(du,8)\n" + "(du,9)\n" + "(du,10)\n" + "(du,11)\n"
			+ "(sogar,1)\n" + "(geht,1)\n" + "(immer,1)\n" + "(immer,2)\n" + "(immer,3)\n" + "(immer,4)\n" + "(mensch,1)\n" + "(mensch,2)\n"
			+ "(befestigt,1)\n" + "(lebt,1)\n" + "(lebt,2)\n" + "(mag,1)\n" + "(mag,2)\n" + "(mag,3)\n" + "(engeln,1)\n" + "(engeln,2)\n"
			+ "(breiten,1)\n" + "(blitzendes,1)\n" + "(tags,1)\n" + "(sie,1)\n" + "(sie,2)\n" + "(plagen,1)\n" + "(plagen,2)\n" + "(allzu,1)\n"
			+ "(meisten,1)\n" + "(o,1)\n" + "(pfade,1)\n" + "(kennst,1)\n" + "(nichts,1)\n" + "(nichts,2)\n" + "(nichts,3)\n" + "(gedanken,1)\n"
			+ "(befriedigt,1)\n" + "(mich,1)\n" + "(mich,2)\n" + "(mich,3)\n" + "(mich,4)\n" + "(mich,5)\n" + "(mich,6)\n" + "(s,1)\n" + "(s,2)\n"
			+ "(s,3)\n" + "(es,1)\n" + "(es,2)\n" + "(es,3)\n" + "(es,4)\n" + "(es,5)\n" + "(es,6)\n" + "(es,7)\n" + "(es,8)\n" + "(verneinen,1)\n"
			+ "(er,1)\n" + "(er,2)\n" + "(er,3)\n" + "(er,4)\n" + "(er,5)\n" + "(er,6)\n" + "(er,7)\n" + "(er,8)\n" + "(er,9)\n" + "(er,10)\n"
			+ "(er,11)\n" + "(er,12)\n" + "(er,13)\n" + "(gleich,1)\n" + "(baeumchen,1)\n" + "(donnergang,1)\n" + "(wunderlich,1)\n"
			+ "(reise,1)\n" + "(urquell,1)\n" + "(doch,1)\n" + "(doch,2)\n" + "(doch,3)\n" + "(aufs,1)\n" + "(aufs,2)\n" + "(toten,1)\n"
			+ "(niemals,1)\n" + "(eine,1)\n" + "(eine,2)\n" + "(hab,1)\n" + "(darfst,1)\n" + "(da,1)\n" + "(da,2)\n" + "(da,3)\n" + "(da,4)\n"
			+ "(da,5)\n" + "(gen,1)\n" + "(einem,1)\n" + "(einem,2)\n" + "(teil,1)\n" + "(das,1)\n" + "(das,2)\n" + "(das,3)\n" + "(das,4)\n"
			+ "(das,5)\n" + "(das,6)\n" + "(das,7)\n" + "(speise,1)\n" + "(wenig,1)\n" + "(sterne,1)\n" + "(geb,1)\n" + "(welten,1)\n"
			+ "(alle,1)\n" + "(alle,2)\n" + "(alle,3)\n" + "(toent,1)\n" + "(gras,1)\n" + "(felsen,1)\n" + "(kette,1)\n" + "(ich,1)\n"
			+ "(ich,2)\n" + "(ich,3)\n" + "(ich,4)\n" + "(ich,5)\n" + "(ich,6)\n" + "(ich,7)\n" + "(ich,8)\n" + "(ich,9)\n" + "(ich,10)\n"
			+ "(ich,11)\n" + "(ich,12)\n" + "(ich,13)\n" + "(ich,14)\n" + "(fuer,1)\n" + "(fuer,2)\n" + "(als,1)\n" + "(als,2)\n" + "(als,3)\n"
			+ "(mein,1)\n" + "(schoene,1)\n" + "(verzeih,1)\n" + "(schwankender,1)\n" + "(wie,1)\n" + "(wie,2)\n" + "(wie,3)\n" + "(wie,4)\n"
			+ "(wie,5)\n" + "(wie,6)\n" + "(wie,7)\n" + "(wie,8)\n" + "(wie,9)\n" + "(menschlich,1)\n" + "(gaertner,1)\n" + "(taetigkeit,1)\n"
			+ "(bange,1)\n" + "(liebe,1)\n" + "(sei,1)\n" + "(sei,2)\n" + "(seh,1)\n" + "(tollheit,1)\n" + "(am,1)\n" + "(am,2)\n" + "(am,3)\n"
			+ "(am,4)\n" + "(am,5)\n" + "(am,6)\n" + "(michael,1)\n" + "(geist,1)\n" + "(ab,1)\n" + "(nahst,1)\n" + "(vollendet,1)\n"
			+ "(liebt,1)\n" + "(brausen,1)\n" + "(nase,1)\n" + "(erlaubt,1)\n" + "(weiss,1)\n" + "(weiss,2)\n" + "(schnellem,1)\n"
			+ "(deinem,1)\n" + "(gleichem,1)\n" + "(gaerung,1)\n" + "(dauernden,1)\n" + "(deines,1)\n" + "(vorgeschriebne,1)\n"
			+ "(irdisch,1)\n" + "(worte,1)\n" + "(verehren,1)\n" + "(hohen,1)\n" + "(hohen,2)\n" + "(weise,1)\n" + "(weise,2)\n"
			+ "(kuenft,1)\n" + "(werdende,1)\n" + "(wette,1)\n" + "(wette,2)\n" + "(wuetend,1)\n" + "(erscheinung,1)\n" + "(gar,1)\n"
			+ "(gar,2)\n" + "(verlieren,1)\n" + "(braucht,1)\n" + "(weiter,1)\n" + "(trank,1)\n" + "(tierischer,1)\n" + "(wohl,1)\n"
			+ "(verteilen,1)\n" + "(verhoehnt,1)\n" + "(schaeumt,1)\n" + "(himmelslichts,1)\n" + "(unbedingte,1)\n" + "(herzlich,1)\n"
			+ "(anblick,1)\n" + "(anblick,2)\n" + "(nennt,1)\n" + "(gruent,1)\n" + "(bluet,1)\n" + "(leichnam,1)\n" + "(erschlaffen,1)\n"
			+ "(jammertagen,1)\n" + "(zieh,1)\n" + "(ihm,1)\n" + "(ihm,2)\n" + "(ihm,3)\n" + "(besondre,1)\n" + "(ihn,1)\n" + "(ihn,2)\n"
			+ "(ihn,3)\n" + "(ihn,4)\n" + "(ihn,5)\n" + "(grossen,1)\n" + "(vollen,1)\n" + "(ihr,1)\n" + "(ihr,2)\n" + "(ihr,3)\n"
			+ "(ihr,4)\n" + "(ihr,5)\n" + "(ihr,6)\n" + "(ihr,7)\n" + "(boten,1)\n" + "(voller,1)\n" + "(singt,1)\n" + "(muhme,1)\n"
			+ "(schon,1)\n" + "(last,1)\n" + "(kleine,1)\n" + "(paradieseshelle,1)\n" + "(nein,1)\n" + "(echten,1)\n" + "(unter,1)\n"
			+ "(bei,1)\n" + "(herr,1)\n" + "(herr,2)\n" + "(herr,3)\n" + "(herr,4)\n" + "(herr,5)\n" + "(herr,6)\n" + "(herr,7)\n"
			+ "(herr,8)\n" + "(herr,9)\n" + "(herr,10)\n" + "(herr,11)\n" + "(gern,1)\n" + "(gern,2)\n" + "(gern,3)\n" + "(sphaerenlauf,1)\n"
			+ "(stets,1)\n" + "(ganze,1)\n" + "(braechte,1)\n" + "(fordert,1)\n" + "(schoensten,1)\n" + "(herrlich,1)\n" + "(herrlich,2)\n"
			+ "(gegeben,1)\n" + "(allein,1)\n" + "(allein,2)\n" + "(reichen,1)\n" + "(schauervoller,1)\n" + "(musst,1)\n" + "(recht,1)\n"
			+ "(bleibt,1)\n" + "(pracht,1)\n" + "(treibt,1)\n" + "(befangen,1)\n" + "(was,1)\n" + "(was,2)\n" + "(menschen,1)\n"
			+ "(menschen,2)\n" + "(menschen,3)\n" + "(jede,1)\n" + "(hohe,1)\n" + "(tiefsten,1)\n" + "(bilden,1)\n" + "(drum,1)\n"
			+ "(gibt,1)\n" + "(gibt,2)\n" + "(guter,1)\n" + "(fuerwahr,1)\n" + "(im,1)\n" + "(im,2)\n" + "(im,3)\n" + "(grund,1)\n"
			+ "(in,1)\n" + "(in,2)\n" + "(in,3)\n" + "(in,4)\n" + "(in,5)\n" + "(in,6)\n" + "(in,7)\n" + "(in,8)\n" + "(in,9)\n" + "(hoechste,1)\n"
			+ "(schliesst,1)\n" + "(fels,1)\n" + "(steh,1)\n" + "(euer,1)\n" + "(erster,1)\n" + "(ersten,1)\n" + "(ersten,2)\n" + "(ersten,3)\n"
			+ "(goettersoehne,1)\n" + "(brechen,1)\n" + "(tiefen,1)\n" + "(frucht,1)\n" + "(kreis,1)\n" + "(siehst,1)\n" + "(wege,1)\n"
			+ "(ist,1)\n" + "(ist,2)\n" + "(ist,3)\n" + "(ist,4)\n" + "(ist,5)\n" + "(ist,6)\n" + "(ist,7)\n" + "(ist,8)\n" + "(zikaden,1)\n"
			+ "(frischen,1)\n" + "(ruh,1)\n" + "(deine,1)\n" + "(deine,2)\n" + "(maus,1)\n" + "(brudersphaeren,1)\n" + "(nachher,1)\n"
			+ "(euch,1)\n" + "(euch,2)\n" + "(euch,3)\n" + "(euch,4)\n" + "(gnaden,1)\n" + "(anzuklagen,1)\n" + "(schlange,1)\n" + "(staerke,1)\n"
			+ "(staerke,2)\n" + "(erde,1)\n" + "(erde,2)\n" + "(erde,3)\n" + "(erde,4)\n" + "(verlaub,1)\n" + "(sanfte,1)\n" + "(holden,1)\n"
			+ "(sonst,1)\n" + "(treten,1)\n" + "(sahst,1)\n" + "(alten,1)\n" + "(um,1)\n" + "(wieder,1)\n" + "(alter,1)\n" + "(altes,1)\n"
			+ "(nun,1)\n" + "(lieb,1)\n" + "(gesellen,1)\n" + "(erscheinen,1)\n" + "(wirkt,1)\n" + "(wirkt,2)\n" + "(haettst,1)\n" + "(haettst,2)\n"
			+ "(nur,1)\n" + "(nur,2)\n" + "(nur,3)\n" + "(nur,4)\n" + "(nur,5)\n" + "(nur,6)\n" + "(nur,7)\n" + "(tiefbewegte,1)\n" + "(lachen,1)\n"
			+ "(lachen,2)\n" + "(drange,1)\n" + "(schlag,1)\n" + "(schein,1)\n" + "(muss,1)\n" + "(verworren,1)\n" + "(weges,1)\n" + "(allen,1)\n"
			+ "(gewoehnlich,1)\n" + "(alles,1)\n" + "(halb,1)\n" + "(stuerme,1)\n" + "(springt,1)\n" + "(sollt,1)\n" + "(klarheit,1)\n"
			+ "(so,1)\n" + "(so,2)\n" + "(so,3)\n" + "(so,4)\n" + "(so,5)\n" + "(so,6)\n" + "(erfassen,1)\n" + "(liedchen,1)\n" + "(prolog,1)\n"
			+ "(zur,1)\n" + "(fressen,1)\n" + "(zum,1)\n" + "(faust,1)\n" + "(faust,2)\n" + "(erzengel,1)\n" + "(erzengel,2)\n" + "(jahre,1)\n"
			+ "(sonn,1)\n" + "(raphael,1)\n" + "(land,1)\n" + "(land,2)\n" + "(lang,1)\n" + "(gelange,1)\n" + "(lust,1)\n" + "(lust,2)\n"
			+ "(welt,1)\n" + "(sehe,1)\n" + "(ihre,1)\n" + "(jedes,1)\n" + "(erfreut,1)\n" + "(seiner,1)\n" + "(denn,1)\n" + "(wandeln,1)\n"
			+ "(wechselt,1)\n" + "(jeden,1)\n" + "(dort,1)\n" + "(schlecht,1)\n" + "(wenigsten,1)\n" + "(wuerd,1)\n" + "(schranken,1)\n"
			+ "(bewusst,1)\n" + "(bewusst,2)\n" + "(seinem,1)\n" + "(seinem,2)\n" + "(gehasst,1)\n" + "(sein,1)\n" + "(meinem,1)\n"
			+ "(meinen,1)\n" + "(pathos,1)\n" + "(herrn,1)\n" + "(lange,1)\n" + "(lange,2)\n" + "(herab,1)\n" + "(diesen,1)\n" + "(ihren,1)\n"
			+ "(beruehmte,1)\n" + "(goethe,1)\n" + "(tag,1)\n" + "(tag,2)\n" + "(tag,3)\n" + "(tier,1)\n" + "(quark,1)\n" + "(dank,1)\n"
			+ "(seine,1)\n" + "(teufel,1)\n" + "(teufel,2)\n" + "(zweck,1)\n" + "(wenn,1)\n" + "(wenn,2)\n" + "(wenn,3)\n" + "(wenn,4)\n"
			+ "(wenn,5)\n" + "(wenn,6)\n" + "(wenn,7)\n" + "(soll,1)\n" + "(wirkung,1)\n" + "(erlaubnis,1)\n" + "(lebendig,1)\n" + "(uns,1)\n"
			+ "(leicht,1)\n" + "(gewiss,1)\n" + "(schnell,1)\n" + "(und,1)\n" + "(und,2)\n" + "(und,3)\n" + "(und,4)\n" + "(und,5)\n" + "(und,6)\n"
			+ "(und,7)\n" + "(und,8)\n" + "(und,9)\n" + "(und,10)\n" + "(und,11)\n" + "(und,12)\n" + "(und,13)\n" + "(und,14)\n" + "(und,15)\n"
			+ "(und,16)\n" + "(und,17)\n" + "(und,18)\n" + "(und,19)\n" + "(und,20)\n" + "(und,21)\n" + "(und,22)\n" + "(und,23)\n" + "(und,24)\n"
			+ "(und,25)\n" + "(und,26)\n" + "(und,27)\n" + "(und,28)\n" + "(und,29)\n" + "(gerne,1)\n" + "(rechten,1)\n" + "(umher,1)\n" + "(umher,2)\n"
			+ "(vernunft,1)\n" + "(grase,1)\n" + "(nach,1)\n" + "(leben,1)\n" + "(gott,1)\n" + "(der,1)\n" + "(der,2)\n" + "(der,3)\n" + "(der,4)\n"
			+ "(der,5)\n" + "(der,6)\n" + "(der,7)\n" + "(der,8)\n" + "(der,9)\n" + "(der,10)\n" + "(der,11)\n" + "(der,12)\n" + "(der,13)\n"
			+ "(der,14)\n" + "(der,15)\n" + "(der,16)\n" + "(der,17)\n" + "(der,18)\n" + "(der,19)\n" + "(der,20)\n" + "(der,21)\n" + "(der,22)\n"
			+ "(der,23)\n" + "(der,24)\n" + "(der,25)\n" + "(der,26)\n" + "(der,27)\n" + "(der,28)\n" + "(der,29)\n" + "(des,1)\n" + "(des,2)\n"
			+ "(des,3)\n" + "(des,4)\n" + "(des,5)\n" + "(doktor,1)\n" + "(beschaemt,1)\n" + "(dreht,1)\n" + "(habe,1)\n" + "(sagen,1)\n" + "(sagen,2)\n"
			+ "(bekennen,1)\n" + "(dunklen,1)\n" + "(wettet,1)\n" + "(den,1)\n" + "(den,2)\n" + "(den,3)\n" + "(den,4)\n" + "(den,5)\n" + "(den,6)\n"
			+ "(den,7)\n" + "(den,8)\n" + "(den,9)\n" + "(mephistopheles,1)\n" + "(mephistopheles,2)\n" + "(mephistopheles,3)\n"
			+ "(mephistopheles,4)\n" + "(mephistopheles,5)\n" + "(mephistopheles,6)\n" + "(mephistopheles,7)\n" + "(mephistopheles,8)\n"
			+ "(mephistopheles,9)\n" + "(dem,1)\n" + "(dem,2)\n" + "(dem,3)\n" + "(dem,4)\n" + "(auch,1)\n" + "(auch,2)\n" + "(auch,3)\n" + "(auch,4)\n"
			+ "(kann,1)\n" + "(kann,2)\n" + "(armen,1)\n" + "(mir,1)\n" + "(mir,2)\n" + "(mir,3)\n" + "(mir,4)\n" + "(mir,5)\n" + "(mir,6)\n" + "(mir,7)\n"
			+ "(mir,8)\n" + "(mir,9)\n" + "(strebt,1)\n" + "(gut,1)\n" + "(gut,2)\n" + "(mit,1)\n" + "(mit,2)\n" + "(mit,3)\n" + "(mit,4)\n" + "(mit,5)\n"
			+ "(mit,6)\n" + "(mit,7)\n" + "(mit,8)\n" + "(mit,9)\n" + "(mit,10)\n" + "(mit,11)\n" + "(bald,1)\n" + "(bald,2)\n" + "(himmlischen,1)\n"
			+ "(himmel,1)\n" + "(himmel,2)\n" + "(himmel,3)\n" + "(noch,1)\n" + "(noch,2)\n" + "(noch,3)\n" + "(kannst,1)\n" + "(deinesgleichen,1)\n"
			+ "(flammt,1)\n" + "(ergruenden,1)\n" + "(ergruenden,2)\n" + "(nacht,1)\n" + "(scheint,1)\n" + "(ferne,1)\n" + "(ferne,2)\n"
			+ "(tragoedie,1)\n" + "(abgewoehnt,1)\n" + "(reizt,1)\n" + "(geistern,1)\n" + "(nicht,1)\n" + "(nicht,2)\n" + "(nicht,3)\n" + "(nicht,4)\n"
			+ "(nicht,5)\n" + "(nicht,6)\n" + "(nicht,7)\n" + "(nicht,8)\n" + "(nicht,9)\n" + "(nicht,10)\n" + "(sacht,1)\n" + "(unbegreiflich,1)\n"
			+ "(unbegreiflich,2)\n" + "(schnelle,1)\n" + "(einmal,1)\n" + "(werd,1)\n" + "(werke,1)\n" + "(werke,2)\n" + "(begraebt,1)\n"
			+ "(knecht,1)\n" + "(rings,1)\n" + "(wird,1)\n" + "(katze,1)\n" + "(huete,1)\n" + "(fortgerissen,1)\n" + "(gebt,1)\n" + "(huebsch,1)\n"
			+ "(hast,1)\n" + "(irrt,1)\n" + "(befinde,1)\n" + "(sind,1)\n" + "(sind,2)\n" + "(fuehren,1)\n" + "(fuehren,2)\n" + "(fliegt,1)\n"
			+ "(ewig,1)\n" + "(ewig,2)\n" + "(ewig,3)\n" + "(brust,1)\n" + "(brust,2)\n" + "(sonne,1)\n" + "(sprechen,1)\n" + "(ein,1)\n" + "(ein,2)\n"
			+ "(ein,3)\n" + "(strasse,1)\n" + "(von,1)\n" + "(von,2)\n" + "(von,3)\n" + "(von,4)\n" + "(von,5)\n" + "(von,6)\n" + "(von,7)\n" + "(von,8)\n"
			+ "(ueberlassen,1)\n" + "(dir,1)\n" + "(dir,2)\n" + "(dir,3)\n" + "(dir,4)\n" + "(vom,1)\n" + "(vom,2)\n" + "(vom,3)\n" + "(zu,1)\n" + "(zu,2)\n"
			+ "(zu,3)\n" + "(zu,4)\n" + "(zu,5)\n" + "(zu,6)\n" + "(zu,7)\n" + "(zu,8)\n" + "(zu,9)\n" + "(zu,10)\n" + "(zu,11)\n" + "(schwebt,1)\n"
			+ "(die,1)\n" + "(die,2)\n" + "(die,3)\n" + "(die,4)\n" + "(die,5)\n" + "(die,6)\n" + "(die,7)\n" + "(die,8)\n" + "(die,9)\n" + "(die,10)\n"
			+ "(die,11)\n" + "(die,12)\n" + "(die,13)\n" + "(die,14)\n" + "(die,15)\n" + "(die,16)\n" + "(die,17)\n" + "(die,18)\n" + "(die,19)\n"
			+ "(die,20)\n" + "(die,21)\n" + "(die,22)\n" + "(vor,1)\n" + "(vor,2)\n" + "(wangen,1)\n" + "(wettgesang,1)\n" + "(donnerschlags,1)\n"
			+ "(find,1)\n" + "(dich,1)\n" + "(dich,2)\n" + "(dich,3)\n" + "(umfass,1)\n" + "(verboten,1)\n" + "(laeg,1)\n" + "(nie,1)\n" + "(drei,1)\n"
			+ "(drei,2)\n" + "(dauern,1)\n" + "(toren,1)\n" + "(dauert,1)\n" + "(verheeren,1)\n" + "(fliegend,1)\n" + "(aus,1)\n" + "(staub,1)\n"
			+ "(fluessen,1)\n" + "(haus,1)\n" + "(auf,1)\n" + "(auf,2)\n" + "(auf,3)\n" + "(auf,4)\n" + "(auf,5)\n" + "(dient,1)\n" + "(dient,2)\n"
			+ "(tiefer,1)\n" + "(naeh,1)\n" + "(zieren,1)\n";

	public static final String COUNTS_AS_TUPLES = "(machen,1)\n" + "(zeit,2)\n" + "(heerscharen,1)\n" + "(keiner,2)\n" + "(meine,3)\n"
			+ "(fuehr,1)\n" + "(triumph,1)\n" + "(kommst,1)\n" + "(frei,1)\n" + "(schaffen,1)\n" + "(gesinde,1)\n"
			+ "(langbeinigen,1)\n" + "(schalk,1)\n" + "(besser,1)\n" + "(solang,1)\n" + "(meer,4)\n" + "(fragst,1)\n"
			+ "(gabriel,1)\n" + "(selbst,2)\n" + "(bin,1)\n" + "(sich,7)\n" + "(du,11)\n" + "(sogar,1)\n" + "(geht,1)\n"
			+ "(immer,4)\n" + "(mensch,2)\n" + "(befestigt,1)\n" + "(lebt,2)\n" + "(mag,3)\n" + "(engeln,2)\n" + "(breiten,1)\n"
			+ "(blitzendes,1)\n" + "(tags,1)\n" + "(sie,2)\n" + "(plagen,2)\n" + "(allzu,1)\n" + "(meisten,1)\n" + "(o,1)\n"
			+ "(pfade,1)\n" + "(kennst,1)\n" + "(nichts,3)\n" + "(gedanken,1)\n" + "(befriedigt,1)\n" + "(mich,6)\n" + "(s,3)\n"
			+ "(es,8)\n" + "(verneinen,1)\n" + "(er,13)\n" + "(gleich,1)\n" + "(baeumchen,1)\n" + "(donnergang,1)\n"
			+ "(wunderlich,1)\n" + "(reise,1)\n" + "(urquell,1)\n" + "(doch,3)\n" + "(aufs,2)\n" + "(toten,1)\n" + "(niemals,1)\n"
			+ "(eine,2)\n" + "(hab,1)\n" + "(darfst,1)\n" + "(da,5)\n" + "(gen,1)\n" + "(einem,2)\n" + "(teil,1)\n" + "(das,7)\n"
			+ "(speise,1)\n" + "(wenig,1)\n" + "(sterne,1)\n" + "(geb,1)\n" + "(welten,1)\n" + "(alle,3)\n" + "(toent,1)\n"
			+ "(gras,1)\n" + "(felsen,1)\n" + "(kette,1)\n" + "(ich,14)\n" + "(fuer,2)\n" + "(als,3)\n" + "(mein,1)\n"
			+ "(schoene,1)\n" + "(verzeih,1)\n" + "(schwankender,1)\n" + "(wie,9)\n" + "(menschlich,1)\n" + "(gaertner,1)\n"
			+ "(taetigkeit,1)\n" + "(bange,1)\n" + "(liebe,1)\n" + "(sei,2)\n" + "(seh,1)\n" + "(tollheit,1)\n" + "(am,6)\n"
			+ "(michael,1)\n" + "(geist,1)\n" + "(ab,1)\n" + "(nahst,1)\n" + "(vollendet,1)\n" + "(liebt,1)\n" + "(brausen,1)\n"
			+ "(nase,1)\n" + "(erlaubt,1)\n" + "(weiss,2)\n" + "(schnellem,1)\n" + "(deinem,1)\n" + "(gleichem,1)\n"
			+ "(gaerung,1)\n" + "(dauernden,1)\n" + "(deines,1)\n" + "(vorgeschriebne,1)\n" + "(irdisch,1)\n" + "(worte,1)\n"
			+ "(verehren,1)\n" + "(hohen,2)\n" + "(weise,2)\n" + "(kuenft,1)\n" + "(werdende,1)\n" + "(wette,2)\n" + "(wuetend,1)\n"
			+ "(erscheinung,1)\n" + "(gar,2)\n" + "(verlieren,1)\n" + "(braucht,1)\n" + "(weiter,1)\n" + "(trank,1)\n"
			+ "(tierischer,1)\n" + "(wohl,1)\n" + "(verteilen,1)\n" + "(verhoehnt,1)\n" + "(schaeumt,1)\n" + "(himmelslichts,1)\n"
			+ "(unbedingte,1)\n" + "(herzlich,1)\n" + "(anblick,2)\n" + "(nennt,1)\n" + "(gruent,1)\n" + "(bluet,1)\n"
			+ "(leichnam,1)\n" + "(erschlaffen,1)\n" + "(jammertagen,1)\n" + "(zieh,1)\n" + "(ihm,3)\n" + "(besondre,1)\n"
			+ "(ihn,5)\n" + "(grossen,1)\n" + "(vollen,1)\n" + "(ihr,7)\n" + "(boten,1)\n" + "(voller,1)\n" + "(singt,1)\n"
			+ "(muhme,1)\n" + "(schon,1)\n" + "(last,1)\n" + "(kleine,1)\n" + "(paradieseshelle,1)\n" + "(nein,1)\n" + "(echten,1)\n"
			+ "(unter,1)\n" + "(bei,1)\n" + "(herr,11)\n" + "(gern,3)\n" + "(sphaerenlauf,1)\n" + "(stets,1)\n" + "(ganze,1)\n"
			+ "(braechte,1)\n" + "(fordert,1)\n" + "(schoensten,1)\n" + "(herrlich,2)\n" + "(gegeben,1)\n" + "(allein,2)\n"
			+ "(reichen,1)\n" + "(schauervoller,1)\n" + "(musst,1)\n" + "(recht,1)\n" + "(bleibt,1)\n" + "(pracht,1)\n"
			+ "(treibt,1)\n" + "(befangen,1)\n" + "(was,2)\n" + "(menschen,3)\n" + "(jede,1)\n" + "(hohe,1)\n" + "(tiefsten,1)\n"
			+ "(bilden,1)\n" + "(drum,1)\n" + "(gibt,2)\n" + "(guter,1)\n" + "(fuerwahr,1)\n" + "(im,3)\n" + "(grund,1)\n" + "(in,9)\n"
			+ "(hoechste,1)\n" + "(schliesst,1)\n" + "(fels,1)\n" + "(steh,1)\n" + "(euer,1)\n" + "(erster,1)\n" + "(ersten,3)\n"
			+ "(goettersoehne,1)\n" + "(brechen,1)\n" + "(tiefen,1)\n" + "(frucht,1)\n" + "(kreis,1)\n" + "(siehst,1)\n"
			+ "(wege,1)\n" + "(ist,8)\n" + "(zikaden,1)\n" + "(frischen,1)\n" + "(ruh,1)\n" + "(deine,2)\n" + "(maus,1)\n"
			+ "(brudersphaeren,1)\n" + "(nachher,1)\n" + "(euch,4)\n" + "(gnaden,1)\n" + "(anzuklagen,1)\n" + "(schlange,1)\n"
			+ "(staerke,2)\n" + "(erde,4)\n" + "(verlaub,1)\n" + "(sanfte,1)\n" + "(holden,1)\n" + "(sonst,1)\n" + "(treten,1)\n"
			+ "(sahst,1)\n" + "(alten,1)\n" + "(um,1)\n" + "(wieder,1)\n" + "(alter,1)\n" + "(altes,1)\n" + "(nun,1)\n" + "(lieb,1)\n"
			+ "(gesellen,1)\n" + "(erscheinen,1)\n" + "(wirkt,2)\n" + "(haettst,2)\n" + "(nur,7)\n" + "(tiefbewegte,1)\n"
			+ "(lachen,2)\n" + "(drange,1)\n" + "(schlag,1)\n" + "(schein,1)\n" + "(muss,1)\n" + "(verworren,1)\n" + "(weges,1)\n"
			+ "(allen,1)\n" + "(gewoehnlich,1)\n" + "(alles,1)\n" + "(halb,1)\n" + "(stuerme,1)\n" + "(springt,1)\n" + "(sollt,1)\n"
			+ "(klarheit,1)\n" + "(so,6)\n" + "(erfassen,1)\n" + "(liedchen,1)\n" + "(prolog,1)\n" + "(zur,1)\n" + "(fressen,1)\n"
			+ "(zum,1)\n" + "(faust,2)\n" + "(erzengel,2)\n" + "(jahre,1)\n" + "(sonn,1)\n" + "(raphael,1)\n" + "(land,2)\n"
			+ "(lang,1)\n" + "(gelange,1)\n" + "(lust,2)\n" + "(welt,1)\n" + "(sehe,1)\n" + "(ihre,1)\n" + "(jedes,1)\n"
			+ "(erfreut,1)\n" + "(seiner,1)\n" + "(denn,1)\n" + "(wandeln,1)\n" + "(wechselt,1)\n" + "(jeden,1)\n" + "(dort,1)\n"
			+ "(schlecht,1)\n" + "(wenigsten,1)\n" + "(wuerd,1)\n" + "(schranken,1)\n" + "(bewusst,2)\n" + "(seinem,2)\n"
			+ "(gehasst,1)\n" + "(sein,1)\n" + "(meinem,1)\n" + "(meinen,1)\n" + "(pathos,1)\n" + "(herrn,1)\n" + "(lange,2)\n"
			+ "(herab,1)\n" + "(diesen,1)\n" + "(ihren,1)\n" + "(beruehmte,1)\n" + "(goethe,1)\n" + "(tag,3)\n" + "(tier,1)\n"
			+ "(quark,1)\n" + "(dank,1)\n" + "(seine,1)\n" + "(teufel,2)\n" + "(zweck,1)\n" + "(wenn,7)\n" + "(soll,1)\n"
			+ "(wirkung,1)\n" + "(erlaubnis,1)\n" + "(lebendig,1)\n" + "(uns,1)\n" + "(leicht,1)\n" + "(gewiss,1)\n"
			+ "(schnell,1)\n" + "(und,29)\n" + "(gerne,1)\n" + "(rechten,1)\n" + "(umher,2)\n" + "(vernunft,1)\n" + "(grase,1)\n"
			+ "(nach,1)\n" + "(leben,1)\n" + "(gott,1)\n" + "(der,29)\n" + "(des,5)\n" + "(doktor,1)\n" + "(beschaemt,1)\n"
			+ "(dreht,1)\n" + "(habe,1)\n" + "(sagen,2)\n" + "(bekennen,1)\n" + "(dunklen,1)\n" + "(wettet,1)\n" + "(den,9)\n"
			+ "(mephistopheles,9)\n" + "(dem,4)\n" + "(auch,4)\n" + "(kann,2)\n" + "(armen,1)\n" + "(mir,9)\n" + "(strebt,1)\n"
			+ "(gut,2)\n" + "(mit,11)\n" + "(bald,2)\n" + "(himmlischen,1)\n" + "(himmel,3)\n" + "(noch,3)\n" + "(kannst,1)\n"
			+ "(deinesgleichen,1)\n" + "(flammt,1)\n" + "(ergruenden,2)\n" + "(nacht,1)\n" + "(scheint,1)\n" + "(ferne,2)\n"
			+ "(tragoedie,1)\n" + "(abgewoehnt,1)\n" + "(reizt,1)\n" + "(geistern,1)\n" + "(nicht,10)\n" + "(sacht,1)\n"
			+ "(unbegreiflich,2)\n" + "(schnelle,1)\n" + "(einmal,1)\n" + "(werd,1)\n" + "(werke,2)\n" + "(begraebt,1)\n"
			+ "(knecht,1)\n" + "(rings,1)\n" + "(wird,1)\n" + "(katze,1)\n" + "(huete,1)\n" + "(fortgerissen,1)\n" + "(gebt,1)\n"
			+ "(huebsch,1)\n" + "(hast,1)\n" + "(irrt,1)\n" + "(befinde,1)\n" + "(sind,2)\n" + "(fuehren,2)\n" + "(fliegt,1)\n"
			+ "(ewig,3)\n" + "(brust,2)\n" + "(sonne,1)\n" + "(sprechen,1)\n" + "(ein,3)\n" + "(strasse,1)\n" + "(von,8)\n"
			+ "(ueberlassen,1)\n" + "(dir,4)\n" + "(vom,3)\n" + "(zu,11)\n" + "(schwebt,1)\n" + "(die,22)\n" + "(vor,2)\n"
			+ "(wangen,1)\n" + "(wettgesang,1)\n" + "(donnerschlags,1)\n" + "(find,1)\n" + "(dich,3)\n" + "(umfass,1)\n"
			+ "(verboten,1)\n" + "(laeg,1)\n" + "(nie,1)\n" + "(drei,2)\n" + "(dauern,1)\n" + "(toren,1)\n" + "(dauert,1)\n"
			+ "(verheeren,1)\n" + "(fliegend,1)\n" + "(aus,1)\n" + "(staub,1)\n" + "(fluessen,1)\n" + "(haus,1)\n" + "(auf,5)\n"
			+ "(dient,2)\n" + "(tiefer,1)\n" + "(naeh,1)\n" + "(zieren,1)\n";

}

<div id="top"></div>
<!--
*** Thanks for checking out the readme-file! ;)
-->



<!-- PROJECT SHIELDS -->
[![Contributors][contributors-shield]][contributors-url]



<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/nicolas-ott/dwl">
    <img src="images/Bild1.png">
  </a>
<h3 align="center">Effects on Motorized Individual Transport in the City of Zurich</h3>
</div>



<!-- TABLE OF CONTENTS -->
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
      <a href="#research-questions">Research questions</a>
    </li>
        <li>
      <a href="#goals">Goals</a>
    </li>
        <li>
      <a href="#files-description">Files description</a>
    </li>
        <li>
      <a href="#how-to-run-the-code">How to run the code</a>
    </li>
        <li>
      <a href="#authors">Authors</a>
    </li>
  </ol>



<!-- ABOUT THE PROJECT -->
## About The Project

Motorized individual transport in Switzerland is subject to constant public interest. Be it due to population growth, urban density, or current climate policy. With the outbreak of the Corona pandemic, traffic volumes in Switzerland fell in 2020 for the first time since the mid-1990s. During the pandemic, increased numbers of people switched from public transportation to their own cars or bicycles. As a result, the share of public transport in total motorized land transport fell to 17%, the same as in 2000. With the current easing of the pandemic situation, traffic congestion from motorized individual transport is expected to return to pre-Corona levels.

On the other hand, the outbreak of war in Ukraine on February 23, 2022, led to a drastic increase in the price of oil and, consequently, to rising gasoline prices throughout Europe. While many people have no other option than to commute by car, there is still and especially in the cities the possibility to switch to public transport.

Against this background and to meet the challenges of a growing mobility volume, the project team is interested in the possible factors influencing motorized individual traffic. In doing so, it focuses on the situation in the city of Zurich. By means of qualitative analysis, the motorized individual transport will be examined over a period of ten years. What influence does the weather have? What differences can be observed between working days and weekends? How sensitive is individual traffic in Zurich to increased prices? The findings should deepen the understanding of motorized individual transport and help to promote new mobility concepts in general and especially for the city of Zurich. The work is primarily aimed at public authorities and mobility service providers in the Zurich region.

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- RESEARCH QUESTIONS -->
## Research questions

* Is there an effect of weather conditions on motorized individual transport in the city of Zurich?
* Is there a temporal effect (days of the week, vacation periods, intraday) on motorized individual transport in the city of Zurich?
* Is there an effect of rising oil prices on motorized individual transport in the city of Zurich?

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- GOALS -->
## Goals

The goal of this project is to set up functioning ETL processes using Amazon and other services in order to answer the above reasearch questions. The data is fetched from public API's such as [opendata.swiss](https://opendata.swiss/de/dataset/daten-der-verkehrszahlung-stundenwerte-seit-2012) and [Yahoo Finance API](https://www.yahoofinanceapi.com/).

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- FILES DESCRIPTION -->
## Files description

The repository has the following file structure:

- [ ] [data](https://github.com/nicolas-ott/dwl/tree/main/data)
- [ ] [dominik](https://github.com/nicolas-ott/dwl/tree/main/dominik)
    - [ ] [dags](https://github.com/nicolas-ott/dwl/tree/main/dominik/dags)
    - [ ] [jupyter](https://github.com/nicolas-ott/dwl/tree/main/dominik/Jupyter)
- [ ] [lars](https://github.com/nicolas-ott/dwl/tree/main/lars)
    - [ ] [jupyter](https://github.com/nicolas-ott/dwl/tree/main/lars/Jupyter)
    - [ ] [lambdas](https://github.com/nicolas-ott/dwl/tree/main/lars/Lambdas)
- [ ] [nicolas](https://github.com/nicolas-ott/dwl/tree/main/nicolas)
    - [ ] [jupyter](https://github.com/nicolas-ott/dwl/tree/main/nicolas/Jupyter)
    - [ ] [lambdas](https://github.com/nicolas-ott/dwl/tree/main/nicolas/Lambdas)
- [ ] [images](https://github.com/nicolas-ott/dwl/tree/main/images)

The data folder contains a csv file with the schedule of vacations and school-free days in the city of Zurich. This data was stored manually in an S3 bucket and helps to check for temporal effects on motorized individual transport in the city of Zurich. Each author dealt with another ETL process and created two folders for it: The 'Jupyter'-folders contain several jupyter notebooks. These notebooks were used for data analysis and as a general playground before the code was pushed into production via Amazon Lambda functions or Apache Airflow dags. The 'dags' respectively 'lambda' folders contain the actual code which was deployed using the mentioned services. The image folder contains a nice traffic image. :D

<p align="right">(<a href="#top">back to top</a>)</p>



<!-- HOW TO RUN THE CODE -->
## How to run the code

To be able to run the code the credentials for the different databases and other services were hard coded in the jupyter notebook files. This is not the case for the productive code where the environment variables were set in the respective service (i.e. Lambda, Apache Airflow).


<p align="right">(<a href="#top">back to top</a>)</p>


<!-- AUTHORS -->
## Authors

* Lars Neyerlin - lars.neyerlin@stud.hslu.ch
* Dominik Finzer - dominik.finzer@stud.hslu.ch
* Nicolas Ott - nicolas.ott@stud.hslu.ch

<p align="right">(<a href="#top">back to top</a>)</p>


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/nicolas-ott/dwl.svg?style=for-the-badge
[contributors-shield]: https://img.shields.io/github/contributors/nicolas-ott/dwl.svg?style=for-the-badge
[contributors-url]: https://github.com/nicolas-ott/dwl/graphs/contributors

// This is the main Node.js source code file of your actor.

// Import Apify SDK. For more information, see https://sdk.apify.com/
const Apify = require('apify');
const {gotScraping} = require('got-scraping');
const batchSize = 500;
const axios = require('axios');

const uradPraceAPIUrl = 'https://www.uradprace.cz/volna-mista/rest/volna-mista/dto/query';
const umimePecovatAPIUrl = 'https://api-umime-pecovat.eu.contember.cloud/content/umime-pecovat/live';
const uradPraceJSONUrl = 'https://data.mpsv.cz/od/soubory/volna-mista/volna-mista.json';



const translateText = async (text, proxyConfiguration)=>
{
    if(!text) return '';
    let retry = 0;
    while(retry<4)
    {
        try
        {
            const proxyUrl = proxyConfiguration.newUrl();
            const {body}  = await gotScraping({
                url:'https://translator.cuni.cz/api/v2/languages/?src=cs&tgt=uk&frontend=u4u',
                method:'POST',
                proxyUrl,
                headers:{'Content-Type': 'application/x-www-form-urlencoded'},
                body:`input_text=${text}&logInput=true&inputType=keyboard&author=`,
                retry: { limit: 5 }
            });
            return body.trim().replaceAll('"','');
        }
        catch(error)
        {
            console.log('error '+ error.message +' retryCount:'+retry);
            retry++;
        }
    }
}


const getDateString = (value) =>
{
    return (new Date(value)).toISOString().replace('T',' ').split('.')[0];
}


const getSourceJobs = async () =>
{
    const result = [];
    const graphQLQuery = {"index":["volna-mista"],"pagination":{"start":0,"count":batchSize,"order":["-datumZmeny","-id"]},"aggs":{"sumField":{"sum":{"field":"volneMisto.pocetMist"}}},"query":{"must":[{"match":{"field":"oboryCinnosti.oborCinnostiProVmId","query":"4086"}},{"mustNot":[{"match":{"field":"zverejnovatVpmId","query":3721}}]},{"match":{"field":"stavVolnehoMistaId","query":3701}},{"should":[{"match":{"field":"expirace","query":null}},{"range":{"field":"expirace","gte":"2022-05-28"}}]},{"range":{"field":"pocetMist","gte":1}}]}};
    const {body:jobs} = await gotScraping({url:uradPraceAPIUrl, method:'POST',useHeaderGenerator: false,json:graphQLQuery, responseType:'json'});
  
    result.push(...jobs.list);
    while(jobs.count > result.length)
    {
        graphQLQuery.pagination.start = result.length;
        const {body:batch} = await gotScraping({url:uradPraceAPIUrl, method:'POST',useHeaderGenerator: false,json:graphQLQuery, responseType:'json'});
        if(!batch.list.length)
        {
            return result;
        }
        result.push(...batch.list);
    }
    const response = await axios.get(uradPraceJSONUrl);
    const polozky = response.data.polozky;
    const polozkyHash = {};
    polozky.forEach((x)=>polozkyHash[x.portalId]=x);
    result.forEach((x)=>{
        const polozka = polozkyHash[x.id];
        if(!polozka)
        {
           console.log('did not find polozka id:'+x.id);
           fillJobFromPolozka(x,{});
           return;
        }
        fillJobFromPolozka(x,polozka);
    
    })

    return result;
}

const fillJobFromPolozka = (job,polozka)=>
{
    job.statniSpravaSamosprava = polozka.statniSpravaSamosprava ?? null;
    job.terminUkonceniPracovnihoPomeru = polozka.terminUkonceniPracovnihoPomeru?? null;
    job.terminZahajeniPracovnihoPomeru = polozka.terminZahajeniPracovnihoPomeru?? null;
    job.souhlasAgenturyAgentura = polozka.souhlasAgenturyAgentura?? null;
    job.souhlasAgenturyUzivatel = polozka.souhlasAgenturyUzivatel?? null;
    job.upresnujiciInformace = polozka.upresnujiciInformace?.cs?? '';
    job.urlAdresa = polozka.urlAdresa?? '';
    job.kontaktniPracoviste = polozka.kontaktniPracoviste?.id?? '';
    job.portalId = polozka.portalId?? '';
    
    job.pracovisteUlice = polozka.mistoVykonuPrace?.pracoviste?.[0]?.adresa?.ulice?.nazev?? '';
    job.pracovisteCisloDomovni = polozka.mistoVykonuPrace?.pracoviste?.[0]?.adresa?.cisloDomovni?? '';
    job.pracovisteCisloOrientacni =polozka.mistoVykonuPrace?.pracoviste?.[0]?.adresa?.cisloOrientacni?? '';
    job.pracovistePSC = polozka.mistoVykonuPrace?.pracoviste?.[0]?.adresa?.psc?? '';

    job.pracovnePravniVztahy = polozka.pracovnePravniVztahy?.[0]?.id?? '';
    job.pocetHodinTydne = polozka.pocetHodinTydne?? '';
    job.kontaktniOsobaZamestnavateleJmeno = polozka?.kontaktniOsobaZamestnavatele?.jmeno?? '';
    job.kontaktniOsobaZamestnavatelePrijmeni = polozka?.kontaktniOsobaZamestnavatele?.prijmeni?? '';
    job.kontaktniOsobaZamestnavateleEmail = polozka?.kontaktniOsobaZamestnavatele?.email?? '';
    job.kontaktniOsobaZamestnavateleTelefon = polozka?.kontaktniOsobaZamestnavatele?.telefon?? '';
    job.kontaktniOsobaZamestnavatelePozice = polozka?.kontaktniOsobaZamestnavatele?.poziceVeSpolecnosti?.cs?? '';
    
}



const getTargetJobs = async () =>
{
    
    const result = [];
    const graphQLQuery = {"query":"query { listOffer { id,identifier,pozadovanaProfese, nazevPracoviste, isDeleted}}"};
    const {body:jobs} = await gotScraping(
        {
            url:umimePecovatAPIUrl,
            method:'POST',
            useHeaderGenerator: false,
            json:graphQLQuery,
            responseType:'json',
            headers: {Authorization: `Bearer ${process.env.UMIME_PECOVAT_API_KEY}`, 'Content-Type': 'application/json'}});
    result.push(...jobs.data.listOffer);
    return result;
}

const compare = async (sourceJobs,targetJobs) =>
{
    const newJobs = [];
    const existsJobs = [];
    
    for(const sourceJob of sourceJobs)
    {

        const targetJob = targetJobs.find((x)=>x.identifier == sourceJob.id);

        if(targetJob)
        {
           existsJobs.push(sourceJob);
        }
        else
        {
            newJobs.push(sourceJob);
        }
    }

    const deletedJobs = sourceJobs.filter((x)=>!x.isDeleted && !sourceJobs.find((y)=>y.identifier == x.identifier));
    return { newJobs, existsJobs, deletedJobs };
    
}

const callGraphQL = async(query)=>
{
    let retry = 0;
    while(retry<4)
    {
        try{
            const {body:response} = await gotScraping(
                {
                    url:umimePecovatAPIUrl,
                    method:'POST',
                    useHeaderGenerator: false,
                    json:query,
                    responseType:'json',
                    headers: {Authorization: `Bearer ${process.env.UMIME_PECOVAT_API_KEY}`, 'Content-Type': 'application/json'}
                });
            if(query.query.startsWith('mutation'))
            {
                return response;
            }
            return Object.values(response.data)?.[0]?.[0];
        }
        catch(error)
        {
            
            console.log('error callGraphQL:'+error.message+' query:'+ query+ ' retryCount:'+retry);
            retry++;
        }
    }

}

const getDataQueryPart = async(job)=>
{
    let districtLine = '';
    if(job.mistoVykonuPrace?.obec?.okres?.nazev)
    {
    //district
        const districtQuery = {query:`query {
            listOfferDistrict(filter: { name: { eq: "${job.mistoVykonuPrace.obec.okres.nazev}" } }) {
            id
            }
        }`};
        const districtResponse = await callGraphQL(districtQuery);
        districtLine = districtResponse? `okres: { connect: { id: "${districtResponse.id}" } }`:`okres: { create: { name: "${job.mistoVykonuPrace.obec.okres.nazev}" } }`;
    }
    
    //profession
    const professionQuery = {query:`query {
        listOfferProfession(filter: { nazev: { eq: "${job.profeseCzIsco.nazev}" } }) {
          id
        }
      }`};

    const professionResponse = await callGraphQL(professionQuery);
    const professionLine = professionResponse? `profese: { connect: { id: "${professionResponse.id}" } }`:`profese: { create: { kod: ${job.profeseCzIsco.kod}, nazev: "${job.profeseCzIsco.nazev}" } }`;

    let employerLine = '';
    //employer
    if(job.zamestnavatel?.ico)
    {
        const employerQuery = {query:`query {
            listOfferEmployer(filter: { ico: { eq: "${job.zamestnavatel.ico}" } }) {
            id
            }
        }`};

        const employerResponse = await callGraphQL(employerQuery);
        employerLine = employerResponse? `zamestnavatel: { connect: { id: "${employerResponse.id}" } }`:`zamestnavatel: { create: { identifier: ${job.zamestnavatel.id}, nazev: "${job.zamestnavatel.nazev.replaceAll('"','')}", ico: "${job.zamestnavatel.ico}" } }`;
    }

    const expiraceLine = job.expirace?  `expirace: "${job.expirace}"`:'';

    const terminUkonceniPracovnihoPomeruLine = job.terminUkonceniPracovnihoPomeru? `terminUkonceniPracovnihoPomeru: "${job.terminUkonceniPracovnihoPomeru}"`:'';
    const terminZahajeniPracovnihoPomeruLine = job.terminZahajeniPracovnihoPomeru? `terminZahajeniPracovnihoPomeru: "${job.terminZahajeniPracovnihoPomeru}"`:'';


    const dataPart = `data: {
        identifier: "${job.id}"
        referencniCislo: "${job.referencniCislo}"
        pocetMist: ${job.pocetMist}
        pozadovanaProfese: "${job.pozadovanaProfese.replaceAll('"','')}"
        pozadovanaProfeseUK: "${job.pozadovanaProfeseUK.replaceAll('"','')}"
        vyhotovil: "${job.vyhotovil}"
        vyhotovilUK: "${job.vyhotovilUK}"
        cizinecMimoEu: ${job.cizinecMimoEu}
        zamestnaneckaKarta: ${job.zamestnaneckaKarta}
        modraKarta: ${job.modraKarta}
        azylant: ${job.azylant}
        stavVCentralniEvidenci: "${job.stavVCentralniEvidenci}"
        stavVCentralniEvidenciUK: "${job.stavVCentralniEvidenciUK}"
        mesicniMzdaOd: ${job.mesicniMzdaOd}
        mesicniMzdaDo: ${job.mesicniMzdaDo}
        ${expiraceLine}
        datumVlozeni: "${getDateString(job.datumVlozeni)}"
        datumZmeny: "${getDateString(job.datumZmeny)}"
        minPozadovaneVzdelani: "${job.minPozadovaneVzdelani?.kod}"
        minPozadovaneVzdelaniUK: "${job.minPozadovaneVzdelaniUK}"
        typMzdy: ${job.typMzdy.kod}
        smennost: ${job.smennost.kod}
        nazevPracoviste: "${job.mistoVykonuPrace?.pracoviste?.nazev?.replaceAll('"','')}"
        nazevPracovisteUK: "${job.nazevPracovisteUK}"
        ${districtLine}
        ${professionLine}
        ${employerLine}
        statniSpravaSamosprava: ${job.statniSpravaSamosprava}
        ${terminUkonceniPracovnihoPomeruLine}
        ${terminZahajeniPracovnihoPomeruLine}
        souhlasAgenturyAgentura: ${job.souhlasAgenturyAgentura}
        souhlasAgenturyUzivatel: ${job.souhlasAgenturyUzivatel}
        upresnujiciInformace: "${job.upresnujiciInformace?.replaceAll('"','').replace(/(\r\n|\r|\n|\t)/g,' ')}"
        upresnujiciInformaceUK: "${job.upresnujiciInformaceUK?.replace(/(\r\n|\r|\n|\t)/g,' ')}"
        urlAdresa: "${job.urlAdresa}"
        kontaktniPracoviste: "${ job.kontaktniPracoviste}"
        portalId: "${ job.portalId}"
        pracovisteUlice: "${ job.pracovisteUlice}"
        pracovisteCisloDomovni: "${ job.pracovisteCisloDomovni}"
        pracovisteCisloOrientacni: "${job.pracovisteCisloOrientacni}"
        pracovistePSC: "${ job.pracovistePSC}"
        pracovnePravniVztahy: "${ job.pracovnePravniVztahy}"
        pocetHodinTydne: "${ job.pocetHodinTydne}"
        kontaktniOsobaZamestnavateleJmeno: "${ job.kontaktniOsobaZamestnavateleJmeno}"
        kontaktniOsobaZamestnavatelePrijmeni: "${ job.kontaktniOsobaZamestnavatelePrijmeni}"
        kontaktniOsobaZamestnavateleEmail: "${ job.kontaktniOsobaZamestnavateleEmail}"
        kontaktniOsobaZamestnavateleTelefon: "${ job.kontaktniOsobaZamestnavateleTelefon}"
        kontaktniOsobaZamestnavatelePozice: "${ job?.kontaktniOsobaZamestnavatelePozice}"
    }`;
    return dataPart;


}

const insertJob = async(job, proxyConfiguration) =>
{
    await translateJob(job, proxyConfiguration);
    const dataPart = await getDataQueryPart(job);
    const offerQuery = {query: `mutation {
        createOffer (${dataPart}
            ) {
                ok
                errorMessage
            }
    }`};

    
    const responseOffer = await callGraphQL(offerQuery);
    
    return responseOffer;
}

const updateJob = async(job, targetJobs, proxyConfiguration) =>
{
    await translateJob(job, proxyConfiguration);
    const dataPart = await getDataQueryPart(job);
    const targetJob = targetJobs.find((x)=>x.identifier == job.id);
    if(!targetJob)
    {
        throw Error('could not find targetJob for update sourceJobId: '+ job.id);
    }
    
        const updateQuery = { query: `mutation {
        updateOffer (
            by:{id:"${targetJob.id}"},
            ${dataPart}
            
        ) {
            ok
            errorMessage
        }
    }`};

    
    const responseOffer = await callGraphQL(updateQuery);
    
    return responseOffer;
}


const setAsDeletedJob = async(job)=>
{
    const deleteQuery = { query: `mutation {
                    updateOffer (
                        by:{id:"${job.id}"},
                        data: {
                            isDeleted:true
                        }
                    ) {
                        ok
                        errorMessage
                    }
                }`};
    const response = await callGraphQL(deleteQuery);
    if(response.data.updateOffer.ok)
    {
        console.log(`Job "${job.pozadovanaProfese} - ${job.nazevPracoviste}" deleted.`);
    }
    else
    {
        console.log(`Error - could not delete job - "${job.pozadovanaProfese} - ${job.nazevPracoviste}" - ${response.data.updateOffer.errorMessage}`);
    }
}

const translateJob = async(job, proxyConfiguration) =>
{
    const responses = await Promise.all([translateText(job.pozadovanaProfese, proxyConfiguration),
        translateText(job.vyhotovil, proxyConfiguration),
        translateText(job.minPozadovaneVzdelani?.kod, proxyConfiguration),
        translateText(job.mistoVykonuPrace?.pracoviste?.nazev?.replaceAll('"',''), proxyConfiguration),
        translateText(job.stavVCentralniEvidenci, proxyConfiguration),
        translateText(job.upresnujiciInformace, proxyConfiguration),
     
    ]);

    job.pozadovanaProfeseUK = responses[0];
    job.vyhotovilUK = responses[1];
    job.minPozadovaneVzdelaniUK = responses[2];
    job.nazevPracovisteUK = responses[3];
    job.stavVCentralniEvidenciUK  = responses[4];
    job.upresnujiciInformaceUK  = responses[5];

    
    
    
    // job.pozadovanaProfeseUK = await translateText(job.pozadovanaProfese, proxyConfiguration);
    // job.vyhotovilUK = await translateText(job.vyhotovil, proxyConfiguration);
    // job.minPozadovaneVzdelaniUK = await translateText(job.minPozadovaneVzdelani?.kod, proxyConfiguration);
    // job.nazevPracovisteUK = await translateText(job.mistoVykonuPrace?.pracoviste?.nazev?.replaceAll('"',''), proxyConfiguration);
    // job.stavVCentralniEvidenciUK  = await translateText(job.stavVCentralniEvidenci, proxyConfiguration);
}



Apify.main(async () => {
  const proxyConfiguration = await Apify.createProxyConfiguration();
  const sourceJobs = await getSourceJobs();
  const targetJobs = await getTargetJobs();
  const {newJobs, existsJobs, deletedJobs } = await compare(sourceJobs, targetJobs);
   
  for(const job of newJobs)
  {
    const response = await insertJob(job, proxyConfiguration);
    if(response.data?.createOffer.ok)
    {
        console.log(`Job "${job.pozadovanaProfese} - ${job.mistoVykonuPrace?.pracoviste.nazev}" inserted.`);
    }
    else
    {
        console.log(`Error - could not insert job - "${job.pozadovanaProfese} - ${job.mistoVykonuPrace?.pracoviste.nazev}" - ${response.data?.createOffer.errorMessage || JSON.stringify(response)}`);
    }
  }

//   for(const job of existsJobs)
//     {
//         let response;
//         try
//         {
//             response = await updateJob(job, targetJobs, proxyConfiguration);
//             if(response.data?.updateOffer.ok)
//             {
//                 console.log(`Job "${job.pozadovanaProfese} - ${job.mistoVykonuPrace?.pracoviste.nazev}" updated.`);
//             }
//             else
//             {
//                 console.log(`Error - could not update job - "${job.pozadovanaProfese} - ${job.mistoVykonuPrace?.pracoviste.nazev}" - ${response.data?.updateOffer.errorMessage || JSON.stringify(response)}`);
//             }
//         }
//         catch (error){
//             console.log(`Error - could not update job - "${job.pozadovanaProfese} - ${job.mistoVykonuPrace?.pracoviste.nazev}" - ${error.message}`);
//         }
//     }

  for(const job of deletedJobs)
  {
      await setAsDeletedJob(job);
  }
  console.log(`Done .. new ${newJobs.length}, exists ${existsJobs.length}, deleted ${deletedJobs.length}`);

});

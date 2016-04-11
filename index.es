var antlr4 = require('antlr4');
var fhirpath = require('./fhirpath');
var util = require('util');

var coerce = {
    integer: function(v){
        if (!util.isArray(v)) {
            throw new Error("can't boolean coerce nonarray"  + v)
        }
        if (v.length !== 1){
            return NaN
        }
        return parseInt(v[0])
    },
    boolean: function(v){
        if (!util.isArray(v)) {
            throw new Error("can't boolean coerce nonarray"  + v)
        }
        if (v.length === 1 && (v[0] === true || v[0] === false)){
            return v[0]
        }

        if (v.length === 0) {
            return false;
        }

        return true
    }
}

Array.prototype.flatMap = function(lambda) {
    return Array.prototype.concat.apply([], this.map(lambda));
}

var applyToEach = (fn) => (coll, context, ...rest) => {
    return coll.flatMap(item =>fn.apply(null, [item, context].concat(rest)))
}

var resolveArguments = (fn) => (coll, context, ...rest) =>
    fn.apply(null, [coll, context]
                    .concat(rest.map(i =>
                                     run([context.root], withTree(context,i)))))

var uniqueValueMap = (rows) => 
    rows.reduce((all, val)=>{
        all[JSON.stringify(val)]=true
        return all
    }, {})

var getUniqueValues = (rows)=> {
	var unique = {};
	var distinct = [];
	for( var i  = 0; i< rows.length; i++){
		if( typeof(unique[JSON.stringify(rows[i])]) == "undefined"){
		 distinct.push(rows[i]);
		}
		unique[JSON.stringify(rows[i])] = 0;
	}
	return distinct;
}

var allPaths = (item)=>[item]
.concat(util.isArray(item) ? item.flatMap(allPaths) : [])
.concat( typeof item === 'object' && !util.isArray(item)?
        Object
        .keys(item)
        .reduce((coll, k)=> coll.concat(allPaths(item[k])) , []) : [])

var functionBank = {
    "$path": applyToEach((item, context, segment, recurse)=>{
        if (item.resourceType && item.resourceType === segment){
            return item
        }
        var segments = [segment]
        var choice = segment.match(/\[x\]$/)
        if (choice){
            segments = Object.keys(item).filter(k=>k.match(RegExp("^"+choice[1])))
        }
        return segments.flatMap(s=>item[s]).filter(x=> !!x)
    }),
    "$axis": applyToEach((item, context, axis)=>{
        if (axis === "*")
            return (typeof item === "object") ? 
                Object.keys(item).flatMap(s=>item[s]).filter(x=> !!x) : item
        if (axis === "**")
            return allPaths(item).slice(1)
        if (axis === "$context")
            return context.root
        if (axis === "$this")
           return (typeof item === "object") ? 
                Object.keys(item).flatMap(s=>item[s]).filter(x=> !!x) : item
        throw new Error("Unsupported asis: " + axis)
    }),
    "$where": applyToEach((item, context, conditions) =>
        coerce.boolean(run([item], withTree(context,conditions))) ? [item] : []
    ),
     "$select": applyToEach((item, context, conditions) =>
       run([item], withTree(context,conditions))
    ),
    "$constant": (_, context, val)=>{
        return [val]
    },
    "$first": (coll)=> coll.slice(0,1),
	"$single": (coll)=> {
		if(coll.length > 1){
			throw new Error('single: Input collection contains more than one element');
		}
		return coll.slice(0,1);
	},
    "$last": (coll)=> coll.slice(-1),
    "$tail": (coll)=> coll.slice(1),
    "$item": resolveArguments((coll, context, i) => coll.slice(i,i+1)),
    "$skip": resolveArguments((coll, context, i) => coll.slice(i)),
    "$take": resolveArguments((coll, context, i) => coll.slice(0,i)),
    // TODO: Clarify what collections are accepted by substring
    "$substring": resolveArguments((coll, context, start, count) => {
      validateStringManipulation(coll, 'substring');
        var input = coll[0]
        var end = count !== undefined ? start[0] + count[0] : input.length
        if(typeof input !== 'undefined'){
            return [input.slice(start[0], end)]
        }
        return [];
    }),
    "$startsWith": resolveArguments((coll, context, searchColl) => {
       validateStringManipulationWithParam(coll, searchColl, 'startsWith');
        var input = coll[0],
        searchString = searchColl[0];
         if(typeof input !== 'undefined'){
           return [input.indexOf(searchString) === 0];
        }
        return[]
    }),
    "$endsWith": resolveArguments((coll, context, searchColl) => {
        validateStringManipulationWithParam(coll, searchColl, 'endsWith');
        if(typeof coll[0]!== 'undefined'){
            var input = coll[0],
            position = input.length - searchColl[0].length,
            lastIndext = input.indexOf(searchColl[0], position);
            
            return [lastIndext !== -1 && lastIndext === position];
        }
       return [];
    }),
    "$matches": resolveArguments((coll, context, regularEx) => {
        validateStringManipulationWithParam(coll, regularEx, 'matches');
        var input = coll[0],
        regularExInput = regularEx[0];
        if(typeof input !== 'undefined'){
            var matchesResult = input.match(regularExInput);
            if(matchesResult.length == 1){
                return [input === matchesResult[0]];
            }else{
                return [false]
            }
        }
        return [];
    }),
    "$replaceMatches": resolveArguments((coll, context, regularEx, replaceColl) => {
        validateStringManipulationWithParam(coll, replaceColl, 'replaceMatches');
       
        var input = coll[0],
        regularExInput = regularEx[0].replace(/\\\\/g, '\\'),
        replaceString = replaceColl[0],
        re = new RegExp(regularExInput, "g");
         if(typeof input !== 'undefined'){
             
            return [input.replace(re, replaceString)];
        }
        return[];
    }),
    "$replace": resolveArguments((coll, context, regularEx, replaceColl) => {
        validateStringManipulationWithParam(coll, replaceColl, 'replace');
        var input = coll[0],
        regularExInput = regularEx[0],
        replaceString = replaceColl[0];
        if(typeof input != 'undefined'){
             return [replaceAll(input, regularExInput, replaceString)];
        }
       return[];
    }),
     "$subsetOf": resolveArguments((coll, context, paramColl) => {
        var isarraySubset = isSubsetOf(coll, paramColl);
        return [isarraySubset];
    }),
     "$supersetOf": resolveArguments((coll, context, paramColl) => {
      var isarraySubset = isSubsetOf(paramColl, coll);
        return [isarraySubset];
    }),
    "$contains": resolveArguments((coll, context, searchString) => {
         validateStringManipulationWithParam(coll, searchString, 'contains');
        var input = coll[0],
        searchStringInput = searchString[0];
        if(typeof input !== 'undefined'){
           return [input.indexOf(searchString) !== -1]; 
        }
        return[];
    }),
    "$length": resolveArguments((coll, context) => {
        validateStringManipulation(coll, 'length')
        var input = coll[0];
        if(typeof input !== 'undefined'){
            return [input.length];
        }
        return[];
    }),
    "$empty": (coll)=>[coll.length === 0],
    "$not": (coll) => [!coerce.boolean(coll)],
    "$all": (coll) => [coerce.boolean(coll)],
    "$any": (coll, context, conditions) =>
        [functionBank.$where(coll, context, conditions).length > 0],
    "$count": (coll) => [coll.length],
    "$lookup": (coll, context, tag) => [lookup(tag, context)],
    "$iif": (coll, context, criteriumExp, trueExp, otherwiseExpr) => {
        var criteriumResult = run(coll, withTree(context,criteriumExp));
        if(coerce.boolean(criteriumResult)){
            return run(coll, withTree(context,trueExp));
        }else{
            return run(coll, withTree(context,otherwiseExpr));
        }
    },
    // TODO how does asInteger convert "5.6", or *numbers* e.g. from count()
    "$toInteger": (coll)=> {
      return parseNumber(coll, parseInt, 'toInteger', 'toInteger: Incompatible item. Item must be integer, string that is convertible to integer or a boolean (true/false).');     
    },
    "$toDecimal": (coll)=> {        
         return parseNumber(coll, parseFloat, 'toDecimal', 'toDecimal: Incompatible item. Item must be decimal, string that is convertible to decimal or a boolean (true/false).');         
    },
    "$toString": (coll)=>{
        validateOnlyOneElementInCollection(coll, 'toString');
        if(typeof coll[0] !== "object"){
            return coll[0].toString();
        }else{
            throw new Error('toString: Incompatible item. Item must be a string, a number or a boolean (true/false).')
        }
    },
    "$isDistinct":(coll, context, ...rest)=>
        [coll.length ===getUniqueValues(coll).length],
	"$distinct":(coll, context, ...rest)=>
        [getUniqueValues(coll)],
	"$exists":(coll)=>{
        var isEmptyColl = functionBank.$empty(coll);
        return [!isEmptyColl[0]];
    },
   // TODO startsWith probably needs an argument
   // and why does .startsWith act as a filter, while .matches returns a boolean?
   "$today":()=>[new Date().toLocaleDateString()],
   "$now":()=>[new Date().toString()]
}

var whenSingle = (fn)=> (lhs, rhs) => {
        if (lhs.length !== 1 || rhs.length !== 1) return [];
        return fn(lhs[0], rhs[0]);
    }
//TODO: compare types?
var whenSingleComparison = (fn) => (lhs, rhs) =>{
    if(lhs.length === 0  || rhs.length === 0) return[];
    if(lhs.length !== 1 || rhs.length !== 1) return false;
    return fn(lhs[0], rhs[0]);    
}

 function validateMathOperation(lhs, rhs, fnName) {
      
        var errorMessage = checkForMoreThanOneElementInExpression(rhs, lhs, fnName);
        if (errorMessage != null) {
            throw new Error(errorMessage);
        }
        
       
        if(typeof lhs[0] !== typeof rhs[0]){
            throw new Error(fnName+': Operands must be of the same type.');
        }
        
         //TODO: check if both are numbers
 }
 
 function checkForMoreThanOneElementInExpression(lhs, rhs, functName){
        var errorMessage = null;

        if (lhs.length !== 1 || rhs.length !== 1) {
            errorMessage = functName + 'Expression contains more than one element.';
        }
        
        return errorMessage;
 }
 
 function parseNumber(coll, parseFunction, funcName, validationMessage){
         
        validateOnlyOneElementInCollection(coll, funcName);
        if(!isNaN(coll[0]) && typeof coll[0] !== "object"){
            
           if(coll[0] === true){
               return [1];
           } else if( coll[0] === false){
               return [0];
           } else{
               return [parseFunction.call(null,coll[0])];
           }
        } else{
            throw new Error(validationMessage);
        }     
 }
 
function validateStringManipulationWithParam(coll, paramColl, functName){
    validateStringManipulation(coll, functName);
    validateStringManipulationParams(paramColl, functName);
}

function validateStringManipulation(coll, functName){
    checkMoreThanOneElementInCollection(coll, functName, ': input collection contains more than one element.');
    checkIsCompatibleString(coll[0], functName, ': Incompatible item. Item must be of type string.');
}

function validateStringManipulationParams(coll, functName){
    checkMoreThanOneElementInCollection(coll, functName, ': parameter collection contains more than one element.');
    checkIsCompatibleString(coll[0], functName, ': Incompatible item. Parameter must be of type string.');
}

function validateOnlyOneElementInCollection(coll, functName){
    checkMoreThanOneElementInCollection(coll, functName, ': parameter collection contains more than one element.');
}

function checkMoreThanOneElementInCollection(coll, functName, errorMessage){
    if (coll.length > 1) {
        throw new Error(functName + errorMessage);
    }
}

function checkIsCompatibleString(inputValue, functName, errorMessage){   
    if(typeof inputValue !=='undefined' && typeof inputValue !== "string"){
        throw new Error(functName  + errorMessage);
    }
}

function isSubsetOf(inputCollection, destinationCollection){    
      var isarraySubset = inputCollection.every(function(val) { 
            for (var index = 0; index < destinationCollection.length; index++) {
                var element = destinationCollection[index];
                if(JSON.stringify(val) === JSON.stringify(element)){
                    return true;
                }
            }
            return false;
        });
        
        return isarraySubset;
}

function replaceAll(str, find, replace) {
  return str.replace(new RegExp(escapeRegExp(find), 'g'), replace);
}

function escapeRegExp(str) {
    return str.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1");
}
            
var operatorBank = {
    "=": (lhs, rhs) => [JSON.stringify(lhs)===JSON.stringify(rhs)],
    "!=": (lhs, rhs) => operatorBank["="](lhs, rhs).map(x=>!x),
    "|": (lhs, rhs) => {			
			return getUniqueValues(lhs.concat(rhs));
		},
    "+": (lhs, rhs)=>{
        validateMathOperation(lhs, rhs, "+");
        return [lhs[0] + rhs[0]];
    },
	 "*": (lhs, rhs)=>{
        validateMathOperation(lhs, rhs, "*");
        return [lhs[0] * rhs[0]];
    },
	 "/": (lhs, rhs)=>{
        validateMathOperation(lhs, rhs, "/");
        return [lhs[0] / rhs[0]];
    },
    "-": (lhs, rhs)=>{
        validateMathOperation(lhs, rhs, "-");
        return [lhs[0] - rhs[0]];
    },
	"div":  (lhs, rhs)=>{
        validateMathOperation(lhs, rhs, "div");
        return [Math.floor(lhs[0] / rhs[0])];
    },
	"mod": (lhs, rhs)=>{
        validateMathOperation(lhs, rhs, "mod");
        return [lhs[0] % rhs[0]];
    },
    "&": whenSingle((lhs, rhs)=>{
        if (typeof lhs !== typeof rhs) return []
        return [lhs + rhs];
    }),
    "and": (lhs, rhs) => [coerce.boolean(lhs) && coerce.boolean(rhs)],
    "or": (lhs, rhs) => [coerce.boolean(lhs) || coerce.boolean(rhs)],
    "xor": (lhs, rhs) => [coerce.boolean(lhs) !== coerce.boolean(rhs)],
    "implies": (lhs, rhs) => {
        var lhsCoerceResult;
       if(lhs.length ===0){
           if(rhs.length ===0){
               return [];
           } else if(coerce.boolean(rhs) === true){
                return [true];
            }else{
                return[];
            }
       } else{
           lhsCoerceResult  = coerce.boolean(lhs);
            if(lhsCoerceResult === true){
                return [coerce.boolean(rhs)];
           
            }else if(lhsCoerceResult === false){
                return [true];
            }
       }
        },
    "in": (lhs, rhs) => {
        let lhsMap = uniqueValueMap(lhs)
        let rhsMap = uniqueValueMap(rhs)
        return [Object.keys(lhsMap).every((k)=> k in rhsMap)]
    },
    "~": (lhs, rhs)=> [
        JSON.stringify(lhs.map(JSON.stringify).sort()) ===
        JSON.stringify(rhs.map(JSON.stringify).sort())],
    "!~": (lhs, rhs)=> operatorBank["~"](lhs, rhs).map(x=>!x),
    ">": whenSingleComparison((lhs, rhs)=> [lhs > rhs]),
    "<": whenSingleComparison((lhs, rhs)=> [lhs < rhs]),
    ">=": whenSingleComparison((lhs, rhs)=> [lhs >= rhs]),
    "<=": whenSingleComparison((lhs, rhs)=> [lhs <= rhs]),
}

var withTree = (context, tree) => Object.assign({}, context, {tree: tree})

function run(coll, context){

    if (!util.isArray(context.tree[0])){
        return run(coll, withTree(context, [context.tree]));
    }

    return context.tree.reduce((coll, cur)=>{
        if (util.isArray(cur[0])){
            return [coll].concat(run(coll, withTree(context, cur[0])))
        }

        let fnName = cur[0];
        let fn = functionBank[fnName];
        if (fn) {
            return fn.apply(null, [coll, context].concat(cur.slice(1)))
        }

        return operatorBank[fnName](
            run(coll, withTree(context, cur[1])),
            run(coll, withTree(context, cur[2])))
    }, coll);

}

var defaultLookups = {
  "sct": "http://snomed.info/sct",
  "loinc": "http://loinc.org",
  "ucum": "http://unitsofmeasure.org",
  "vs-": "http://hl7.org/fhir/ValueSet/",
  "ext-": "http://hl7.org/fhir/StructureDefinition/"
}

var lookup = (tag, context) => {

    if (context.lookups[tag]){
        return context.lookups[tag]
    }

    let m = tag.match(/(.*?-)(.*)/)
    if (m && context.lookups[m[1]]){
        return context.lookups[m[1]] + m[2]
    }

    throw new Error(`Undefined lookup tag: %${tag}.
                     We know: ${Object.keys(context.lookups)}`)
}

let withConstants = (lookups) =>
({
    parse: (path) => fhirpath.parse(path),
    evaluate: (resource, path, localLookups) =>
    run( [resource], {
        tree: fhirpath.parse(path),
        lookups: Object.assign({}, lookups||{},localLookups||{}, defaultLookups),
        root: resource
    })
})

module.exports = withConstants({})
module.exports.withConstants = withConstants
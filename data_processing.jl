using JSON
using DataFrames
using Query
using CSV

# extract json from 7z
function extract_json(data_path, filepaths_to_extract)
    cd(data_path)
    try
        if typeof(filepaths_to_extract) <: AbstractArray
            for fp in filepaths_to_extract
                Base.run(`7z x $fp -aos`)
            end
        else
            Base.run(`7z x $filepaths_to_extract -aos`)
        end
    catch e    
    end
    cd("..")
    return true
end

# remove json files (use after processing to save disk space)
function remove_json(data_path) 
    to_remove = joinpath.(data_path, [i for i in readdir(data_path) if occursin(".json", i)])
    for i in to_remove
        rm(i)
    end
    return true
end

# read json file from given path
function read_json_sample(path)
    data_sample = Dict()
    open(path, "r") do file
        data_sample = JSON.parse(file)
    end
    return data_sample
end

# test if all elements in a list are equal [FIND REFERENCE]
allequal(x) = all(y -> y == x[1], x)

function read_and_split_raw_data(jsonpath) 
    metadata = read_json_sample(jsonpath)
    results = pop!(metadata)
    return metadata, results
end

function metadata_to_df(metadata) 
    metadata_df_list = DataFrame[]
    for i in 1:length(metadata)
        tmp_df_list = DataFrame[]
        for j in 1:length(metadata[i])
            push!(tmp_df_list, DataFrame(metadata[i][j]))
        end
        tmp_df = vcat(tmp_df_list..., cols=:union)
        push!(metadata_df_list, tmp_df)
    end
    metadata_df = vcat(metadata_df_list..., cols=:union)
    return metadata_df
end

function strip_protocol(s) 
    if startswith(s, "http://")
        return s[8:end]
    elseif startswith(s, "https://")
        return s[9:end]
    else
        return s
    end
end

function strip_network(s)
    if startswith(s, "www.")
        return s[5:end]
    elseif startswith(s, "de-de.")
        return s[7:end]
    elseif startswith(s, "de.")
        return s[4:end]
    else
        return s
    end
end

function results_to_df(results)
    results_df_list = DataFrame[]
    for result_list in results
        tmp_url_list = [strip_network(strip_protocol(i["sourceUrl"])) for i in result_list["result"]]
        tmp_medium_list = [try i["medium"] catch e "NA" end for i in result_list["result"]]
        tmp_domain_list = [split(i, "/")[1] for i in tmp_url_list]
        # skip faulty result lists
        if allequal(tmp_url_list)
            continue
        end
        tmp_df = DataFrame(sourceUrl = tmp_url_list, medium = tmp_medium_list, domain = tmp_domain_list)
        tmp_df[:, :result_hash] .= result_list["result_hash"]
        tmp_df[:, :rank] = 1:length(tmp_url_list)
        push!(results_df_list, tmp_df)
    end
    results_df = vcat(results_df_list..., cols=:union)
    return results_df
end

function reduce_dataset(df)
    df = df |> @filter(_.search_type == "search") |> DataFrame
    cols_to_delete = intersect(names(df), ["search_type", "plugin_id", "plugin_version"])
    select!(df, Not(cols_to_delete))
    return df
end



##### ----------------------------- #####
##### RAW DATA PROCESSING PROCEDURE #####
##### ----------------------------- #####

if !("processed" in readdir()) 
    mkdir("processed")
end
pattern = ".7z"
file_endings = Dict(
    "7z" => ".7z",
    "json" => ".json"
)
data_path = "data"
archive_list = [i for i in readdir(data_path) if occursin(file_endings["7z"], i)]
filenames_map = Dict(
    "Alice Weidel" => "aliceweidel",
    "CSU" => "csu",
    "Christian Lindner" => "christianlindner",
    "Cem Özdemir" => "cemoezdemir",
    "Dietmar Bartsch" => "dietmarbartsch",
    "SPD" => "spd",
    "FDP" => "fdp",
    "CDU" => "cdu",
    "Bündnis90/Die Grünen" => "gruene",
    "Alexander Gauland" => "alexandergauland",
    "Angela Merkel" => "angelamerkel",
    "Martin Schulz" => "martinschulz",
    "AfD" => "afd",
    "Katrin Göring-Eckardt" => "katringoeringeckardt",
    "Die Linke" => "linke",
    "Sahra Wagenknecht" => "sahrawagenknecht"
)
keywords = collect(keys(filenames_map))
for archive in archive_list
    # read and seperate metadata and search results
    extract_json(data_path, archive)
    jsonpath = joinpath.(data_path, [i for i in readdir(data_path) if occursin(file_endings["json"], i)])[1]
    metadata, results = read_and_split_raw_data(jsonpath);
    remove_json(data_path)

    # put everything into one dataframe
    metadata = metadata_to_df(metadata)
    results = results_to_df(results)
    day_df = innerjoin(results, metadata, on = :result_hash)
    day_df = reduce_dataset(day_df)

    # create datasets by keyword
    for k in keywords
        filename = filenames_map[k] * ".csv"
        keyword_df = day_df |> @filter(_.keyword == k) |> DataFrame
        if filename in readdir("processed")
            CSV.write(joinpath("processed", filename), keyword_df, append = true)
        else
            CSV.write(joinpath("processed", filename), keyword_df)
        end
    end
end






##### ----------- #####
##### OTHER STUFF #####
##### ----------- #####

# yet another data issue: &sa...&ved -> some urls with google specific paths seem broken
# fix broken urls (or don't break them in the first place)
# remove "sa=.*"r
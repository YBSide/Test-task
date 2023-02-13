import requests
import json
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

def make_df_from_query(query, url):
    r = requests.post(url, json={'query': query})
    json_data = json.loads(r.text)
    df_data = json_data['data'][list(json_data['data'].keys())[0]]
    df = pd.json_normalize(df_data)
    return df

def main():
    engine = create_engine(f'postgresql://root:root@localhost:5431/spacex_db')

    url = 'https://spacex-production.up.railway.app/'

    df_dict = dict()
    tables = ['capsules',
              'cores',
              'dragons',
              'histories',
              'landpads',
              'launches',
              'launchpads',
              'missions',
              'rockets',
              'ships']
    for table_name in tables:
        df_dict[table_name] = make_df_from_query(query_dict[table_name], url)

    for table_name in tables:
        if table_name == 'dragons':
            df_dict[table_name].to_sql(name=table_name,
                                       con=engine,
                                       dtype={'thrusters': sqlalchemy.types.JSON},
                                       if_exists='replace')
        elif table_name == 'rockets':
            df_dict[table_name].to_sql(name=table_name,
                                       con=engine,
                                       dtype={'payload_weights': sqlalchemy.types.JSON},
                                       if_exists='replace')
        elif table_name == 'launches':
            df_dict[table_name].to_sql(name=table_name,
                                       con=engine,
                                       dtype={'rocket.rocket.payload_weights': sqlalchemy.types.JSON},
                                       if_exists='replace')
        else:
            df_dict[table_name].to_sql(name=table_name, con=engine, if_exists='replace')
        print(f'{table_name} table inserted into the postgres')



if __name__ == '__main__':

    query_dict = dict()

    query_dict['capsules'] = '''query {
      capsules {
        dragon {
          active
          crew_capacity
          description
          diameter {
            feet
            meters
          }
          dry_mass_kg
          dry_mass_lb
          first_flight
          heat_shield {
            dev_partner
            material
            size_meters
            temp_degrees
          }
          height_w_trunk {
            feet
            meters
          }
          id
          launch_payload_mass {
            kg
            lb
          }
          launch_payload_vol {
            cubic_feet
            cubic_meters
          }
          name
          orbit_duration_yr
          pressurized_capsule {
            payload_volume {
              cubic_feet
              cubic_meters
            }
          }
          return_payload_mass {
            kg
            lb
          }
          return_payload_vol {
            cubic_feet
            cubic_meters
          }
          sidewall_angle_deg
          thrusters {
            amount
            fuel_1
            fuel_2
            pods
            thrust {
              kN
              lbf
            }
            type
          }
          trunk {
            cargo {
              solar_array
              unpressurized_cargo
            }
            trunk_volume {
              cubic_feet
              cubic_meters
            }
          }
          type
          wikipedia
        }
        id
        landings
        missions {
          flight
          name
        }
        original_launch
        reuse_count
        status
        type
      }
    }'''

    query_dict['cores'] = '''query {
      cores {
        asds_attempts
        asds_landings
        block
        id
        missions {
          flight
          name
        }
        original_launch
        reuse_count
        rtls_attempts
        rtls_landings
        status
        water_landing
      }
    }'''

    query_dict['dragons'] = '''query {
      dragons {
        active
        crew_capacity
        description
        diameter {
          feet
          meters
        }
        dry_mass_kg
        dry_mass_lb
        first_flight
        heat_shield {
          dev_partner
          material
          size_meters
          temp_degrees
        }
        height_w_trunk {
          feet
          meters
        }
        id
        launch_payload_mass {
          kg
          lb
        }
        launch_payload_vol {
          cubic_feet
          cubic_meters
        }
        name
        orbit_duration_yr
        pressurized_capsule {
          payload_volume {
            cubic_feet
            cubic_meters
          }
        }
        return_payload_mass {
          kg
          lb
        }
        return_payload_vol {
          cubic_feet
          cubic_meters
        }
        sidewall_angle_deg
        thrusters {
          amount
          fuel_1
          fuel_2
          pods
          thrust {
            kN
            lbf
          }
          type
        }
        trunk {
          cargo {
            solar_array
            unpressurized_cargo
          }
          trunk_volume {
            cubic_feet
            cubic_meters
          }
        }
        type
        wikipedia
      }
    }'''

    query_dict['histories'] = '''query {
      histories {
        details
        event_date_unix
        event_date_utc
        flight {
          details
          id
          is_tentative
          launch_date_local
          launch_date_unix
          launch_date_utc
          launch_site {
            site_id
            site_name
            site_name_long
          }
          launch_success
          launch_year
          links {
            article_link
            flickr_images
            mission_patch
            mission_patch_small
            presskit
            reddit_campaign
            reddit_launch
            reddit_media
            reddit_recovery
            video_link
            wikipedia
          }
          mission_id
          mission_name
          rocket {
            fairings {
              recovered
              recovery_attempt
              reused
              ship
            }
            first_stage {
              cores {
                block
                core {
                  asds_attempts
                  asds_landings
                  block
                  id
                  original_launch
                  reuse_count
                  rtls_attempts
                  rtls_landings
                  status
                  water_landing
                }
                flight
                gridfins
                land_success
                landing_intent
                landing_type
                landing_vehicle
                legs
                reused
              }
            }
            rocket {
              active
              boosters
              company
              cost_per_launch
              country
              description
              diameter {
                feet
                meters
              }
              engines {
                engine_loss_max
                layout
                number
                propellant_1
                propellant_2
                thrust_sea_level {
                  kN
                  lbf
                }
                thrust_to_weight
                thrust_vacuum {
                  kN
                  lbf
                }
                type
                version
              }
              first_flight
              first_stage {
                burn_time_sec
                engines
                fuel_amount_tons
                reusable
              }
              height {
                feet
                meters
              }
              id
              landing_legs {
                material
                number
              }
              mass {
                kg
                lb
              }
              name
              payload_weights {
                id
                kg
                lb
                name
              }
              second_stage {
                burn_time_sec
                engines
                fuel_amount_tons
                payloads {
                  option_1
                }
                thrust {
                  kN
                  lbf
                }
              }
              stages
              success_rate_pct
              type
              wikipedia
            }
            rocket_name
            rocket_type
            second_stage {
              block
              payloads {
                customers
                id
                manufacturer
                nationality
                norad_id
                orbit
                orbit_params {
                  apoapsis_km
                  arg_of_pericenter
                  eccentricity
                  epoch
                  inclination_deg
                  lifespan_years
                  longitude
                  mean_anomaly
                  mean_motion
                  periapsis_km
                  period_min
                  raan
                  reference_system
                  regime
                  semi_major_axis_km
                }
                payload_mass_kg
                payload_mass_lbs
                payload_type
                reused
              }
            }
          }
          ships {
            abs
            active
            attempted_landings
            class
            course_deg
            home_port
            id
            image
            imo
            missions {
              flight
              name
            }
            mmsi
            model
            name
            position {
              latitude
              longitude
            }
            roles
            speed_kn
            status
            successful_landings
            type
            url
            weight_kg
            weight_lbs
            year_built
          }
          static_fire_date_unix
          static_fire_date_utc
          telemetry {
            flight_club
          }
          tentative_max_precision
          upcoming
        }
        id
        links {
          article
          reddit
          wikipedia
        }
        title
      }
    }'''

    query_dict['landpads'] = '''query {
      landpads {
        attempted_landings
        details
        full_name
        id
        landing_type
        location {
          latitude
          longitude
          name
          region
        }
        status
        successful_landings
        wikipedia
      }
    }'''

    query_dict['launches'] = '''query {
      launches {
        details
        id
        is_tentative
        launch_date_local
        launch_date_unix
        launch_date_utc
        launch_site {
          site_id
          site_name
          site_name_long
        }
        launch_success
        launch_year
        links {
          article_link
          flickr_images
          mission_patch
          mission_patch_small
          presskit
          reddit_campaign
          reddit_launch
          reddit_media
          reddit_recovery
          video_link
          wikipedia
        }
        mission_id
        mission_name
        rocket {
          fairings {
            recovered
            recovery_attempt
            reused
            ship
          }
          first_stage {
            cores {
              block
              core {
                asds_attempts
                asds_landings
                block
                id
                missions {
                  flight
                  name
                }
                original_launch
                reuse_count
                rtls_attempts
                rtls_landings
                status
                water_landing
              }
              flight
              gridfins
              land_success
              landing_intent
              landing_type
              landing_vehicle
              legs
              reused
            }
          }
          rocket {
            active
            boosters
            company
            cost_per_launch
            country
            description
            diameter {
              feet
              meters
            }
            engines {
              engine_loss_max
              layout
              number
              propellant_1
              propellant_2
              thrust_sea_level {
                kN
                lbf
              }
              thrust_to_weight
              thrust_vacuum {
                kN
                lbf
              }
              type
              version
            }
            first_flight
            first_stage {
              burn_time_sec
              engines
              fuel_amount_tons
              reusable
            }
            height {
              feet
              meters
            }
            id
            landing_legs {
              material
              number
            }
            mass {
              kg
              lb
            }
            name
            payload_weights {
              id
              kg
              lb
              name
            }
            second_stage {
              burn_time_sec
              engines
              fuel_amount_tons
              payloads {
                option_1
              }
              thrust {
                kN
                lbf
              }
            }
            stages
            success_rate_pct
            type
            wikipedia
          }
          rocket_name
          rocket_type
          second_stage {
            block
            payloads {
              customers
              id
              manufacturer
              nationality
              norad_id
              orbit
              orbit_params {
                apoapsis_km
                arg_of_pericenter
                eccentricity
                epoch
                inclination_deg
                lifespan_years
                longitude
                mean_anomaly
                mean_motion
                periapsis_km
                period_min
                raan
                reference_system
                regime
                semi_major_axis_km
              }
              payload_mass_kg
              payload_mass_lbs
              payload_type
              reused
            }
          }
        }
        ships {
          abs
          active
          attempted_landings
          class
          course_deg
          home_port
          id
          image
          imo
          missions {
            flight
            name
          }
          mmsi
          model
          name
          position {
            latitude
            longitude
          }
          roles
          speed_kn
          status
          successful_landings
          type
          url
          weight_kg
          weight_lbs
          year_built
        }
        static_fire_date_unix
        static_fire_date_utc
        telemetry {
          flight_club
        }
        tentative_max_precision
        upcoming
      }
    }'''

    query_dict['launchpads'] = '''query {
      launchpads {
        attempted_launches
        details
        id
        location {
          latitude
          longitude
          name
          region
        }
        name
        status
        successful_launches
        vehicles_launched {
          active
          boosters
          company
          cost_per_launch
          country
          description
          diameter {
            feet
            meters
          }
          engines {
            engine_loss_max
            layout
            number
            propellant_1
            propellant_2
            thrust_sea_level {
              kN
              lbf
            }
            thrust_to_weight
            thrust_vacuum {
              kN
              lbf
            }
            type
            version
          }
          first_flight
          first_stage {
            burn_time_sec
            engines
            fuel_amount_tons
            reusable
          }
          height {
            feet
            meters
          }
          id
          landing_legs {
            material
            number
          }
          mass {
            kg
            lb
          }
          name
          payload_weights {
            id
            kg
            lb
            name
          }
          second_stage {
            burn_time_sec
            engines
            fuel_amount_tons
            payloads {
              option_1
            }
            thrust {
              kN
              lbf
            }
          }
          stages
          success_rate_pct
          type
          wikipedia
        }
        wikipedia
      }
    }'''

    query_dict['missions'] = '''query {
      missions {
        description
        id
        manufacturers
        name
        payloads {
          customers
          id
          manufacturer
          nationality
          norad_id
          orbit
          orbit_params {
            apoapsis_km
            arg_of_pericenter
            eccentricity
            epoch
            inclination_deg
            lifespan_years
            longitude
            mean_anomaly
            mean_motion
            periapsis_km
            period_min
            raan
            reference_system
            regime
            semi_major_axis_km
          }
          payload_mass_kg
          payload_mass_lbs
          payload_type
          reused
        }
        twitter
        website
        wikipedia
      }
    }'''

    query_dict['payloads'] = '''query {
      payloads {
        customers
        id
        manufacturer
        nationality
        norad_id
        orbit
        orbit_params {
          apoapsis_km
          arg_of_pericenter
          eccentricity
          epoch
          inclination_deg
          lifespan_years
          longitude
          mean_anomaly
          mean_motion
          periapsis_km
          period_min
          raan
          reference_system
          regime
          semi_major_axis_km
        }
        payload_mass_kg
        payload_mass_lbs
        payload_type
        reused
      }
    }'''

    query_dict['rockets'] = '''query {
      rockets {
        active
        boosters
        company
        cost_per_launch
        country
        description
        diameter {
          feet
          meters
        }
        engines {
          engine_loss_max
          layout
          number
          propellant_1
          propellant_2
          thrust_sea_level {
            kN
            lbf
          }
          thrust_to_weight
          thrust_vacuum {
            kN
            lbf
          }
          type
          version
        }
        first_flight
        first_stage {
          burn_time_sec
          engines
          fuel_amount_tons
          reusable
        }
        height {
          feet
          meters
        }
        id
        landing_legs {
          material
          number
        }
        mass {
          kg
          lb
        }
        name
        payload_weights {
          id
          kg
          lb
          name
        }
        second_stage {
          burn_time_sec
          engines
          fuel_amount_tons
          payloads {
            option_1
          }
          thrust {
            kN
            lbf
          }
        }
        stages
        success_rate_pct
        type
        wikipedia
      }
    }'''

    query_dict['ships'] = '''query {
      ships {
        abs
        active
        attempted_landings
        class
        course_deg
        home_port
        id
        image
        imo
        missions {
          flight
          name
        }
        mmsi
        model
        name
        position {
          latitude
          longitude
        }
        roles
        speed_kn
        status
        successful_landings
        type
        url
        weight_kg
        weight_lbs
        year_built
      }
    }'''

    main()
